"""
Acceptance test for write concern scenarios, without missed message retrieval
"""

import asyncio
import logging
import subprocess
import sys

import httpx

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
log = logging.getLogger("TEST")


async def wait_for_service(url: str, max_attempts: int = 30):
    """just wait for service to be ready"""
    async with httpx.AsyncClient() as client:
        for _ in range(max_attempts):
            try:
                resp = await client.get(f"{url}/health")
                if resp.status_code == 200:
                    return True
            except Exception:
                await asyncio.sleep(0.5)
    return False


async def send_message(url: str, content: str, w: int, timeout: float = 30.0):
    """send message with write concern


    returns: (success, status_code), None for timeout
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(
                f"{url}/messages", json={"content": content, "w": w}
            )
            return resp.status_code == 200, resp.status_code
        except httpx.TimeoutException:
            return False, None  # None indicates timeout


async def get_messages(url: str) -> list:
    """get all messages from service"""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{url}/messages")
        return resp.json() if resp.status_code == 200 else []


async def run_test():
    try:
        # simulating the start M + s1 (pause s2 first to ensure only 2 nodes)
        log.info("Starting master and secondary1...")
        subprocess.run(
            ["docker", "compose", "up", "-d", "--build", "master", "secondary2"],
            check=True,
            capture_output=True,
        )

        # using pause instead of stop because of the in-memory storage
        subprocess.run(
            ["docker", "compose", "pause", "secondary2"],
            check=True,
            capture_output=True,
        )

        await wait_for_service("http://localhost:8000")
        await wait_for_service("http://localhost:8001")
        await asyncio.sleep(2)

        # (Msg1, w=1) - Ok
        log.info("Sending Msg1 with w=1...")
        ok, status = await send_message("http://localhost:8000", "Msg1", 1)
        assert ok, f"Msg1 failed with status {status}"

        # (Msg2, w=2) - Ok
        log.info("Sending Msg2 with w=2...")
        ok, status = await send_message("http://localhost:8000", "Msg2", 2)
        assert ok, f"Msg2 failed with status {status}"

        # (Msg3, w=3) - should fail (timeout or 502) since only 2 nodes available
        log.info("sending Msg3 with w=3 (expected to fail - only 2 nodes)...")
        ok, status = await send_message("http://localhost:8000", "Msg3", 3)
        if ok:
            log.error("w=3 succeeded but should have failed with only 2 nodes")
            return False
        log.info(
            f"W=3 correctly failed: status={status} (None=timeout, 502=replication failed)"
        )

        # (Msg4, W=1) - Ok
        log.info("Sending Msg4 with w=1...")
        ok, status = await send_message("http://localhost:8000", "Msg4", 1)
        assert ok, f"Msg4 failed with status {status}"

        # wait before unpausing the S2
        # replication_one retry loop might still be running
        await asyncio.sleep(3)

        # start S2
        log.info("Starting s2...")
        result = subprocess.run(
            ["docker", "compose", "unpause", "secondary2"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log.error(f"failed to start s2: {result.stderr}")
            raise RuntimeError(f"failed to start s2: {result.stderr}")

        if not await wait_for_service("http://localhost:8002"):
            raise RuntimeError("s2 failed to become ready")
        await asyncio.sleep(10)

        # check messages on S2
        log.info("checkign messages on s2")
        required_messages = await get_messages("http://localhost:8000")
        required_contents = [msg["content"] for msg in required_messages]
        s2_messages = await get_messages("http://localhost:8002")
        log.error(
            f"No retrieval logic in the second iteration, messages: {s2_messages}, required: {required_contents}"
        )

        # s2_contents = [msg["content"] for msg in s2_messages]

        # if all(msg in s2_contents for msg in required):
        #     log.info(f"s2 has required messages: {s2_contents}")
        # else:
        #     log.error(f"expected {required}, got {s2_contents}")
        #     return False

    except Exception as e:
        log.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        log.info("shuting down..")
        subprocess.run(["docker", "compose", "down"], capture_output=True)


if __name__ == "__main__":
    success = asyncio.run(run_test())
    sys.exit(0 if success else 1)
