import logging

logging.basicConfig(level=logging.INFO)


def read_db(path: str):
    with open(path, "r") as f:
        data = f.read()

    try:
        decoded = bytes.fromhex(data)
        return decoded
    except Exception as e:
        logging.error(f"An error occurred during base64 decoding: {e}")
        return None
