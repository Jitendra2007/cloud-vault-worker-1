"""
Shadow Slave Cloud Migration: Forward Ep 1267→2133 from OLD to NEW channel.
Runs on GitHub Actions cloud worker.

OLD channel: -1004359534500
NEW channel: -1004444017700

Resume from Ep 1267 (350 eps already forwarded locally).
"""

import asyncio
import sys
import os
import re
import json
import time
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeFilename
from telethon.errors import FloodWaitError

EP_RE = re.compile(r'(?:Ep(?:isode)?|E)\s*(\d+)', re.IGNORECASE)

OLD_CHANNEL = -1004359534500
NEW_CHANNEL = -1004444017700

RESUME_FROM_EP = 1267  # Local migration stopped at 1266

PROGRESS_FILE = 'shadow_slave_cloud_progress.json'

def load_progress():
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {
            "last_forwarded_ep": RESUME_FROM_EP - 1,
            "forwarded_count": 0,
            "errors": [],
            "missing_in_old": []
        }

def save_progress(prog):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(prog, f, indent=2)

async def main():
    # Get session from environment (GitHub secret)
    session_str = os.environ.get('VAULT_SESSION', '')
    api_id = int(os.environ.get('API_ID', '36198115'))
    api_hash = os.environ.get('API_HASH', 'b4fb430cbe6a89c925db6a1a7ea9c819')

    if not session_str:
        print("ERROR: VAULT_SESSION not set")
        sys.exit(1)

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        print("ERROR: Session not authorized")
        await client.disconnect()
        sys.exit(1)

    print("=" * 70)
    print("SHADOW SLAVE CLOUD MIGRATION: OLD -> NEW CHANNEL")
    print("=" * 70)

    # Step 1: Scan OLD channel and build episode->message map
    print("\nStep 1: Scanning OLD channel for all episodes...")
    messages = []
    async for m in client.iter_messages(OLD_CHANNEL, limit=5000):
        messages.append(m)
    messages.sort(key=lambda x: x.id)

    ep_to_msg = {}
    for m in messages:
        t = ''
        if m.media and hasattr(m.media, 'document') and m.media.document:
            for a in m.media.document.attributes:
                if isinstance(a, DocumentAttributeAudio) and a.title:
                    t = a.title
                elif isinstance(a, DocumentAttributeFilename) and a.file_name:
                    if not t:
                        t = a.file_name
        if not t and m.message:
            t = m.message

        match = EP_RE.search(t)
        if match:
            ep = int(match.group(1))
            if ep not in ep_to_msg:
                ep_to_msg[ep] = m

    print(f"   Found {len(ep_to_msg)} unique episodes in OLD channel")
    print(f"   Range: Ep {min(ep_to_msg.keys())} -> Ep {max(ep_to_msg.keys())}")

    # Step 2: Load progress
    progress = load_progress()
    start_ep = progress['last_forwarded_ep'] + 1
    max_ep = max(ep_to_msg.keys())

    print(f"\nResuming from Ep {start_ep} -> {max_ep}")

    # Build forward queue
    forward_queue = []
    for ep in range(start_ep, max_ep + 1):
        if ep in ep_to_msg:
            forward_queue.append(ep)

    missing_in_old = [ep for ep in range(start_ep, max_ep + 1) if ep not in ep_to_msg]
    if missing_in_old:
        print(f"Missing in OLD channel (will skip): {missing_in_old}")
        progress['missing_in_old'] = missing_in_old

    print(f"Forward queue: {len(forward_queue)} episodes")

    # Step 3: Forward sequentially
    forwarded_count = progress['forwarded_count']
    total = len(forward_queue)

    for i, ep in enumerate(forward_queue):
        msg = ep_to_msg[ep]
        try:
            await client.forward_messages(NEW_CHANNEL, msg)
            forwarded_count += 1
            progress['last_forwarded_ep'] = ep
            progress['forwarded_count'] = forwarded_count

            if forwarded_count % 25 == 0:
                save_progress(progress)
                print(f"   [{forwarded_count}/{total}] Forwarded Ep {ep}", flush=True)

            # Rate limiting
            await asyncio.sleep(0.8)

        except FloodWaitError as e:
            wait_time = e.seconds + 5
            print(f"   FloodWait {e.seconds}s at Ep {ep}. Waiting {wait_time}s...", flush=True)
            save_progress(progress)
            await asyncio.sleep(wait_time)
            # Retry once
            try:
                await client.forward_messages(NEW_CHANNEL, msg)
                forwarded_count += 1
                progress['last_forwarded_ep'] = ep
                progress['forwarded_count'] = forwarded_count
            except Exception as e2:
                print(f"   FAILED Ep {ep} after retry: {e2}", flush=True)
                progress['errors'].append({"ep": ep, "error": str(e2)})

        except Exception as e:
            print(f"   Error Ep {ep}: {e}", flush=True)
            progress['errors'].append({"ep": ep, "error": str(e)})

    save_progress(progress)

    print(f"\n{'=' * 70}")
    print(f"CLOUD MIGRATION COMPLETE")
    print(f"   Forwarded this run: {forwarded_count - (progress.get('forwarded_count', 0) - forwarded_count)}")
    print(f"   Total forwarded: {forwarded_count}")
    print(f"   Last Ep: {progress['last_forwarded_ep']}")
    print(f"   Errors: {len(progress['errors'])}")
    print(f"   Missing in OLD (need backfill): {missing_in_old}")
    print(f"{'=' * 70}")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
