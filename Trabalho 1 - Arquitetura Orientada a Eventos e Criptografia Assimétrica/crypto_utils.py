import os
import json
import base64
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYS_DIR = os.path.join(SCRIPT_DIR, "keys")
PUBLIC_KEYS_DIR = os.path.join(SCRIPT_DIR, "public_keys")

def generate_keys(user_id):
    """Gera um par de chaves RSA (privada e pública) para um usuário."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    public_key = private_key.public_key()

    # Cria diretórios se não existirem
    os.makedirs(os.path.join(KEYS_DIR, user_id), exist_ok=True)
    os.makedirs(PUBLIC_KEYS_DIR, exist_ok=True)
    
    private_key_path = os.path.join(KEYS_DIR, user_id, f"{user_id}.pem")
    public_key_path = os.path.join(PUBLIC_KEYS_DIR, f"{user_id}.pub")

    # Salva a chave privada em formato PEM
    with open(private_key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))

    # Salva a chave pública em formato PEM
    with open(public_key_path, "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ))
    
    print(f"Chaves geradas para '{user_id}':")
    print(f"  Chave Privada: {private_key_path}")
    print(f"  Chave Pública: {public_key_path}")
    return private_key_path, public_key_path

def get_private_key_path(user_id):
    return os.path.join(KEYS_DIR, user_id, f"{user_id}.pem")

def get_public_key_path(user_id):
    return os.path.join(PUBLIC_KEYS_DIR, f"{user_id}.pub")

def sign_message(private_key_path, message):
    """Assina uma mensagem (dicionário) com a chave privada."""
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    
    # Serializa a mensagem de forma canônica para garantir consistência
    message_bytes = json.dumps(message, sort_keys=True).encode('utf-8')
    
    signature = private_key.sign(
        message_bytes,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')

def verify_signature(public_key_path, message, signature):
    """Verifica a assinatura de uma mensagem com a chave pública."""
    try:
        with open(public_key_path, "rb") as key_file:
            public_key = serialization.load_pem_public_key(
                key_file.read(),
            )
        
        message_bytes = json.dumps(message, sort_keys=True).encode('utf-8')
        signature_bytes = base64.b64decode(signature)
        
        public_key.verify(
            signature_bytes,
            message_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except Exception as e:
        print(f"Erro na verificação da assinatura: {e}")
        return False