import smtplib, imaplib, email, os, sys, yaml, time, hashlib, re
from email.message import EmailMessage
from tqdm import tqdm
import re

imaplib.Commands["ID"] = "AUTH"
CONFIG_FILE = "config.yaml"

def load_config():
    with open(CONFIG_FILE, "r", encoding='utf-8') as f:
        return yaml.safe_load(f)

def calc_hash(file_path):
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
            sha256.update(chunk)
    return md5.hexdigest(), sha256.hexdigest()

def human_speed(bytes_per_sec):
    if bytes_per_sec >= 1024**2:
        return f"{bytes_per_sec/1024**2:.2f} MB/s"
    return f"{bytes_per_sec/1024:.2f} KB/s"

def send_file(cfg, file_path):
    start_time = time.time()
    print(f"[SEND START] {time.strftime('%Y-%m-%d %H:%M:%S')}")

    max_size = cfg["transfer"]["max_size_mb"] * 1024 * 1024
    chunk_size = cfg["transfer"]["chunk_size_mb"] * 1024 * 1024
    allow_split = cfg["transfer"]["allow_split"]

    filesize = os.path.getsize(file_path)
    filename = os.path.basename(file_path)

    md5, sha256 = calc_hash(file_path)
    print(f"[SEND HASH] MD5={md5}, SHA256={sha256}")

    parts = []
    if filesize <= max_size:
        parts = [(file_path, 1, 1)]
    else:
        if not allow_split:
            raise RuntimeError("File too large and splitting disabled")
        with open(file_path, "rb") as f:
            idx = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                part_name = f"{filename}.part{idx}"
                with open(part_name, "wb") as pf:
                    pf.write(chunk)
                parts.append((part_name, idx + 1, None))
                idx += 1
        total = len(parts)
        parts = [(p[0], i + 1, total) for i, p in enumerate(parts)]

    total_parts = len(parts)

    with smtplib.SMTP_SSL(cfg["smtp"]["server"], cfg["smtp"]["port"]) as smtp:
        smtp.login(cfg["email"]["address"], cfg["email"]["password"])
        for file_path_part, idx, total in tqdm(parts, desc="Sending", unit="part"):
            msg = EmailMessage()
            msg["Subject"] = f"{cfg['filter']['subject_tag']} {filename} part {idx}/{total_parts}"
            msg["From"] = cfg["email"]["address"]
            msg["To"] = cfg["email"]["address"]
            msg.set_content("file transfer")
            with open(file_path_part, "rb") as f:
                msg.add_attachment(f.read(), maintype="application", subtype="octet-stream", filename=os.path.basename(file_path_part))
            smtp.send_message(msg)
            if file_path_part.endswith(".part" + str(idx-1)):
                os.remove(file_path_part)

    end_time = time.time()
    elapsed = end_time - start_time
    speed = filesize / elapsed if elapsed > 0 else 0

    print(f"[SEND END] {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[SEND STATS] Time={elapsed:.2f}s Speed={human_speed(speed)} Size={filesize/1024/1024:.2f}MB")


def recv_file(cfg, target_filename):
    start_time = time.time()
    print(f"[RECV START] {time.strftime('%Y-%m-%d %H:%M:%S')}")

    download_dir = cfg["download"]["dir"]
    os.makedirs(download_dir, exist_ok=True)

    mail = imaplib.IMAP4_SSL(cfg["imap"]["server"], cfg["imap"]["port"])
    typ, data = mail.login(cfg["email"]["address"], cfg["email"]["password"])
    if typ != "OK":
        print(f"Failed to login: '{typ}', '{data}'.")
        return
    args = ("name", "XXXX", "contact", cfg["email"]["address"], "version", "1.0.0", "vendor", "myclient")
    typ, dat = mail._simple_command('ID', '("' + '" "'.join(args) + '")')

    typ, data = mail.select("INBOX", readonly=True)
    if typ != "OK":
        print(f"Failed to select INBOX: '{typ}', '{data}'.")
        return

    typ, data = mail.search(None, f'(SUBJECT "{cfg["filter"]["subject_tag"]}")')
    mail_ids = data[0].split()

    total_parts = None
    saved_parts = set()

    # download parts
    for mail_id in reversed(mail_ids):
        typ, msg_data = mail.fetch(mail_id, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])
        subject = msg.get("Subject", "")

        if target_filename not in subject:
            continue

        m = re.search(r"part (\d+)/(\d+)", subject)
        if not m:
            continue

        idx = int(m.group(1))
        total = int(m.group(2))

        if total_parts is None:
            total_parts = total

        part_path = os.path.join(download_dir, f"{target_filename}.part{idx}")

        if os.path.exists(part_path):
            print(f"[SKIP] part {idx} exists")
            saved_parts.add(idx)
            continue

        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                with open(part_path, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"[RECV] Saved part {idx}/{total}")
                saved_parts.add(idx)

        if total_parts and len(saved_parts) == total_parts:
            break

    if not saved_parts:
        print("No parts found")
        return

    missing = [i for i in range(1, total_parts + 1) if i not in saved_parts]
    if missing:
        print("Missing parts:", missing)
        return

    # merge from disk
    output_path = os.path.join(download_dir, target_filename)
    total_size = 0

    with open(output_path, "wb") as out:
        for i in tqdm(range(1, total_parts + 1), desc="Merging", unit="part"):
            part_path = os.path.join(download_dir, f"{target_filename}.part{i}")
            with open(part_path, "rb") as pf:
                data = pf.read()
                out.write(data)
                total_size += len(data)

    # hash after merge
    md5, sha256 = calc_hash(output_path)
    print(f"[RECV HASH] MD5={md5}, SHA256={sha256}")

    # cleanup
    for i in range(1, total_parts + 1):
        os.remove(os.path.join(download_dir, f"{target_filename}.part{i}"))

    end_time = time.time()
    elapsed = end_time - start_time
    speed = total_size / elapsed if elapsed > 0 else 0

    print(f"[RECV END] {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[RECV STATS] Time={elapsed:.2f}s Speed={human_speed(speed)} Size={total_size/1024/1024:.2f}MB")
    print("Saved:", output_path)

def main():
    if len(sys.argv) != 3:
        print("Usage: python mail_transfer.py send|recv filename")
        return
    cfg = load_config()
    mode = sys.argv[1]
    filename = sys.argv[2]
    if mode == "send":
        send_file(cfg, filename)
    elif mode == "recv":
        recv_file(cfg, filename)
    else:
        print("Invalid mode")

if __name__ == "__main__":
    main()