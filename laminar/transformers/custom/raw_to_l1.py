from base64 import b64encode
from Crypto.Cipher import ChaCha20_Poly1305
from Crypto.Random import get_random_bytes
from google.cloud.kms import KeyManagementServiceClient
import copy

# TODO: create router for different data types (i.e. scalar, array, struct, array of struct)

def raw_to_l1_envelope_encryption(data: dict, columns: list, extra_params: dict) -> dict:
    kms_project_id: str = extra_params["kms_project_id"]
    kms_region: str = extra_params["kms_region"]
    kms_key_ring: str = extra_params["kms_key_ring"]
    raw_id: str = extra_params["raw_id"]
    encrypted_data: dict = copy.deepcopy(data)

    dek = get_random_bytes(32)

    for column in columns:
        cipher = ChaCha20_Poly1305.new(key=dek)
        nonce = cipher.nonce
        ciphertext, tag = cipher.encrypt_and_digest(str(encrypted_data[column]).encode())
        encrypted_data[column] = b64encode(tag+nonce+ciphertext).decode()
    
    kms_client = KeyManagementServiceClient()
    wrapped_dek = kms_client.encrypt(
        request={
            'name': kms_client.crypto_key_path(
                kms_project_id, kms_region, kms_key_ring, raw_id
            ), 
            'plaintext': dek
        }
    ).ciphertext

    data.update(encrypted_data)
    data["_wrapped_dek"] = wrapped_dek
    return data
    
