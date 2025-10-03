#!/usr/bin/env python3
"""
make_udev_rule.py

List serial devices, ask user to select one (or paste a serial like "A600eVfR"),
then create /etc/udev/rules.d/99-usb-serial.rules with a stable symlink,
reload udev rules, and trigger them.

Usage:
    sudo python3 make_udev_rule.py
or simply:
    python3 make_udev_rule.py   # script will re-run itself with sudo if needed
"""

import os
import sys
import shutil
import subprocess
import getpass
from datetime import datetime
from pathlib import Path

try:
    import serial.tools.list_ports
except Exception as e:
    print("pyserial is required. Install with: pip install pyserial")
    print("Error:", e)
    sys.exit(1)


UDEV_DIR = Path("/etc/udev/rules.d")
RULE_FILENAME = "99-usb-serial.rules"
RULE_PATH = UDEV_DIR / RULE_FILENAME


def ensure_root():
    """If not running as root, re-exec using sudo."""
    if os.geteuid() != 0:
        print("Root privileges required. Re-running with sudo...")
        # Build command: sudo python <this_script> <args...>
        cmd = ["sudo", sys.executable] + sys.argv
        os.execvp("sudo", cmd)  # replaces current process
        # never returns


def list_ports():
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("No serial ports found on this host.")
        return ports

    print("\nDetected serial devices:\n")
    header = f"{'idx':>3}  {'device':15}  {'vid:pid':11}  {'serial':15}  {'description'}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(ports):
        vidpid = f"{p.vid:04x}:{p.pid:04x}" if (p.vid and p.pid) else "-"
        serial_num = p.serial_number or "-"
        desc = p.description or "-"
        print(f"{i:3}  {p.device:15}  {vidpid:11}  {serial_num:15}  {desc}")
    print()
    return ports


def backup_existing_rule(path: Path):
    if path.exists():
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = path.with_suffix(f".{ts}.bak")
        print(f"Backing up existing rule {path} -> {bak}")
        shutil.copy2(path, bak)


def build_rule_by_serial(serial_value: str, symlink_name: str) -> str:
    # Use ATTRS{serial} match
    # ensure quotes in serial_value are escaped (unlikely but safe)
    serial_escaped = serial_value.replace('"', '\\"')
    rule = f'SUBSYSTEM=="tty", ATTRS{{serial}}=="{serial_escaped}", SYMLINK+="{symlink_name}"\n'
    return rule


def build_rule_by_vidpid(vid: int, pid: int, symlink_name: str) -> str:
    # vid/pid are ints; format as hex without 0x prefix but zero padded
    vid_s = f"{vid:04x}"
    pid_s = f"{pid:04x}"
    rule = f'SUBSYSTEM=="tty", ATTRS{{idVendor}}=="{vid_s}", ATTRS{{idProduct}}=="{pid_s}", SYMLINK+="{symlink_name}"\n'
    return rule

def write_rule_file(content: str, path: Path):
    # Ensure directory exists
    if not path.parent.exists():
        print(f"Creating directory {path.parent}")
        path.parent.mkdir(parents=True, exist_ok=True)

    # Backup only if file exists and this is the first append
    if path.exists():
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = path.with_suffix(f".{ts}.bak")
        print(f"Backing up existing rule {path} -> {bak}")
        shutil.copy2(path, bak)
        mode = "a"  # append
        print(f"Appending new rule to {path}")
    else:
        mode = "w"
        print(f"Creating new udev rule file at {path}")

    # Append header + rule
    with open(path, mode) as f:
        f.write(content)
    # set permissions to 644
    path.chmod(0o644)

def reload_udev():
    print("Reloading udev rules...")
    subprocess.run(["udevadm", "control", "--reload-rules"], check=True)
    print("Triggering udev...")
    subprocess.run(["udevadm", "trigger"], check=True)


def interactive_flow():
    ports = list_ports()
    if not ports:
        print("No serial ports found. If your device is plugged in and still not shown, try running this script after confirming drivers are loaded.")
        sys.exit(1)

    # Prompt user: choose index or paste serial manually
    choice = input("Enter device index to use(e.g. 1): ").strip()

    target_serial = None
    target_vid = None
    target_pid = None

    if choice == "":
        print("No choice entered. Exiting.")
        sys.exit(1)

    if choice.isdigit():
        idx = int(choice)
        if idx < 0 or idx >= len(ports):
            print("Index out of range.")
            sys.exit(1)
        p = ports[idx]
        print(f"Selected device: {p.device}  description={p.description}  serial={p.serial_number}  vid:pid={(p.vid,p.pid)}")
        if p.serial_number:
            target_serial = p.serial_number
        elif p.vid and p.pid:
            target_vid, target_pid = p.vid, p.pid
        else:
            print("Selected device has neither a serial number nor VID/PID. You may enter a serial manually.")
            manual = input("Enter ATTRS{serial} value now (or blank to abort): ").strip()
            if not manual:
                print("No serial entered. Aborting.")
                sys.exit(1)
            target_serial = manual

    # Ask symlink name
    default_name = "scale"
    symlink = input(f"Name the symlink to create in /dev (default: '{default_name}'): ").strip() or default_name
    # sanitize symlink (simple)
    if "/" in symlink or symlink.strip() == "":
        print("Invalid symlink name.")
        sys.exit(1)

    # Build udev rule line(s)
    if target_serial:
        rule = build_rule_by_serial(target_serial, symlink)
        print("\nGenerated rule (by serial):")
        print(rule.strip())
    else:
        rule = build_rule_by_vidpid(target_vid, target_pid, symlink)
        print("\nGenerated rule (by VID:PID):")
        print(rule.strip())

    # ask confirmation
    confirm = input(f"Write rule to {RULE_PATH} ? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted by user.")
        sys.exit(0)

    # Write file (wrap with header comment)
    header = (
        "# Created by make_udev_rule.py\n"
        f"# Generated: {datetime.now().isoformat()}\n"
    )
    content = header + rule
    write_rule_file(content, RULE_PATH)

    # reload udev rules
    try:
        reload_udev()
    except subprocess.CalledProcessError as e:
        print("Failed to reload/trigger udev. You may need to run these commands manually:")
        print("  sudo udevadm control --reload-rules")
        print("  sudo udevadm trigger")
        print("Error:", e)
        sys.exit(1)

    print(f"\nDone. You should now see /dev/{symlink} (or it will be created when device is re-plugged).")
    print("To check mapping now, run: ls -l /dev | grep", symlink)


def main():
    # ensure we are root so we can write to /etc/udev and call udevadm
    if os.geteuid() != 0:
        # re-run via sudo
        # If sudo not available, just warn and exit
        if shutil.which("sudo") is None:
            print("This script must be run as root, and sudo is not available.")
            print("Please run: sudo python3", " ".join(sys.argv))
            sys.exit(1)
        # Re-exec with sudo
        try:
            print("Re-running with sudo...")
            os.execvp("sudo", ["sudo", sys.executable] + sys.argv)
        except Exception as e:
            print("Failed to re-run with sudo:", e)
            sys.exit(1)
    # we are root here
    try:
        interactive_flow()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
