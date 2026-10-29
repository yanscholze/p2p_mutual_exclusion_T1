"""Mensagens do protocolo: JSON canônico, RSA-2048/SHA-256 e estado por recurso.

A chave privada nunca sai do processo. PEM e base64 atravessam o Pyro5 como
texto, evitando a conversão de bytes em dicionários pelo serializador serpent.
"""

import base64
import json
from dataclasses import dataclass, field
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

RECURSOS = ("R1", "R2")
PEERS = ("PeerA", "PeerB", "PeerC")
TIPOS = ("PEDIDO", "OK", "LIBERACAO")
CAMPOS = {"remetente", "destinatario", "tipo", "recurso", "timestamp",
          "pedido_timestamp", "pedido_id"}


@dataclass
class EstadoRecurso:
    """Tudo que é específico de R1 ou R2; acessar sob o lock do peer."""

    estado: str = "LIBERADO"
    pedido_timestamp: Optional[int] = None
    pedido_id: Optional[str] = None
    permissoes: set = field(default_factory=set)
    adiados: dict = field(default_factory=dict)


def serializar(conteudo):
    """Garante os mesmos bytes na assinatura e na verificação, em qualquer peer."""
    return json.dumps(conteudo, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True).encode("utf-8")


def gerar_chave():
    """Gera um par RSA novo para cada execução, como solicitado no enunciado."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def chave_publica_pem(privada):
    """Exporta somente a parte pública, no formato PEM SubjectPublicKeyInfo."""
    return privada.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def carregar_chave(pem):
    """Aceita apenas chaves públicas RSA de pelo menos 2048 bits."""
    chave = serialization.load_pem_public_key(pem.encode("ascii"))
    if not isinstance(chave, rsa.RSAPublicKey) or chave.key_size < 2048:
        raise ValueError("a chave deve ser RSA com pelo menos 2048 bits")
    return chave


def assinar(privada, conteudo):
    """Assina todos os campos; PKCS1v15/SHA256 equivale a SHA256withRSA."""
    assinatura = privada.sign(serializar(conteudo), padding.PKCS1v15(), hashes.SHA256())
    return {"conteudo": dict(conteudo),
            "assinatura": base64.b64encode(assinatura).decode("ascii")}


def verificar(chave, envelope):
    """Lança InvalidSignature/ValueError se bytes ou conteúdo foram adulterados."""
    assinatura = base64.b64decode(envelope["assinatura"], validate=True)
    chave.verify(assinatura, serializar(envelope["conteudo"]),
                 padding.PKCS1v15(), hashes.SHA256())


def validar_formato(envelope, tipo, destinatario):
    """Valida o contrato antes de usar campos recebidos em estado compartilhado."""
    if not isinstance(envelope, dict) or set(envelope) != {"conteudo", "assinatura"}:
        raise ValueError("envelope malformado")
    m = envelope["conteudo"]
    if not isinstance(m, dict) or set(m) != CAMPOS:
        raise ValueError("campos inesperados no conteúdo")
    if m["tipo"] != tipo or tipo not in TIPOS:
        raise ValueError("tipo incompatível com o método remoto")
    if m["remetente"] not in PEERS or m["remetente"] == destinatario:
        raise ValueError("remetente desconhecido")
    if m["destinatario"] != destinatario or m["recurso"] not in RECURSOS:
        raise ValueError("destinatário ou recurso inválido")
    for campo in ("timestamp", "pedido_timestamp"):
        if type(m[campo]) is not int or m[campo] <= 0:
            raise ValueError("timestamp deve ser inteiro positivo")
    if m["pedido_timestamp"] > m["timestamp"]:
        raise ValueError("timestamp do pedido está no futuro")
    origem = m["remetente"] if tipo == "PEDIDO" else destinatario
    if m["pedido_id"] != f"{origem}:{m['recurso']}:{m['pedido_timestamp']}":
        raise ValueError("identificador não corresponde ao pedido")
    if tipo == "PEDIDO" and m["timestamp"] != m["pedido_timestamp"]:
        raise ValueError("o pedido deve carregar seu timestamp original")
    if not isinstance(envelope["assinatura"], str):
        raise ValueError("a assinatura deve ser texto base64")
    return m
