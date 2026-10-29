"""Ricart e Agrawala P2P para dois recursos, com RSA e callbacks assíncronos.

Execute em três terminais: python peer.py PeerA / PeerB / PeerC.
Apenas os métodos decorados com @expose pertencem à interface remota.
Os comandos do menu são locais; não há coordenador de permissões.
"""

import argparse
import copy
import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import Pyro5.api
import Pyro5.errors
from cryptography.exceptions import InvalidSignature

import servico_nomes as sn
from protocolo import (PEERS, RECURSOS, EstadoRecurso, assinar, carregar_chave,
                       chave_publica_pem, gerar_chave, validar_formato, verificar)


class Peer:
    """Estado de um processo; menu, workers e handlers compartilham este objeto.

    O RLock protege relógio, filas, chaves, permissões e logs. Chamadas remotas
    SEMPRE acontecem depois de soltá-lo. Cada worker cria seu próprio Proxy.
    """

    def __init__(self, nome, caminho_log=None, silencioso=False):
        if nome not in PEERS:
            raise ValueError("nome de peer inválido")
        self.nome = nome
        self.lock = threading.RLock()
        self.relogio = 0
        self.recursos = {r: EstadoRecurso() for r in RECURSOS}
        self.outros_peers = {}  # nome -> URI resolvida pelo serviço de nomes
        self.chaves_publicas = {}
        self._pems = {}
        self._chave_privada = gerar_chave()
        self._pem = chave_publica_pem(self._chave_privada)
        self._chaves_enviadas = False
        self._ultimos_pedidos = {}  # impede replays de pedidos já processados
        self._eventos = deque(maxlen=5000)
        self._sequencia = 0
        self._silencioso = silencioso
        self._workers = ThreadPoolExecutor(max_workers=6, thread_name_prefix=nome)
        self._log = None
        if caminho_log:
            caminho_log = Path(caminho_log)
            caminho_log.parent.mkdir(parents=True, exist_ok=True)
            self._log = caminho_log.open("a", encoding="utf-8", buffering=1)
        self._registrar("CHAVE_GERADA", algoritmo="RSA-2048/SHA256")

    def _registrar(self, evento, **dados):
        """Registra uma linha legível e JSONL sem exportar chaves privadas."""
        with self.lock:
            self._sequencia += 1
            registro = {"seq": self._sequencia, "peer": self.nome,
                        "evento": evento, "relogio": self.relogio,
                        "instante_ns": time.time_ns(), **dados}
            self._eventos.append(registro)
            if self._log:
                self._log.write(json.dumps(registro, ensure_ascii=False) + "\n")
            if not self._silencioso:
                detalhes = " ".join(f"{k}={v}" for k, v in dados.items())
                print(f"\n[{self.nome} L={self.relogio}] {evento} {detalhes}", flush=True)

    @Pyro5.api.expose
    def get_nome(self):
        """Identificação da mesma interface usada por todos os peers."""
        return self.nome

    @Pyro5.api.expose
    def ola(self, remetente):
        """Conserva a mensagem de diagnóstico do esqueleto (fora do protocolo)."""
        self._registrar("OLA", remetente=remetente)

    @Pyro5.api.expose
    def receber_chave_publica(self, remetente, pem):
        """Bootstrap: guarda a chave do peer conhecido, sem permitir substituição."""
        if remetente not in PEERS or remetente == self.nome or not isinstance(pem, str):
            return False
        try:
            chave = carregar_chave(pem)
        except (ValueError, TypeError, UnicodeError):
            self._registrar("CHAVE_REJEITADA", remetente=remetente)
            return False
        with self.lock:
            if remetente in self._pems and self._pems[remetente] != pem:
                self._registrar("CHAVE_REJEITADA", remetente=remetente,
                                motivo="chave já cadastrada")
                return False
            self.chaves_publicas[remetente] = chave
            self._pems[remetente] = pem
            self._registrar("CHAVE_RECEBIDA", remetente=remetente)
        return True

    def trocar_chaves(self):
        """Envia a chave pública unicast; só libera o menu com as duas recebidas."""
        for nome, uri in self.outros_peers.items():
            with Pyro5.api.Proxy(uri) as outro:
                if not outro.receber_chave_publica(self.nome, self._pem):
                    raise RuntimeError(f"{nome} recusou a chave pública")
            self._registrar("CHAVE_ENVIADA", destinatario=nome)
        with self.lock:
            self._chaves_enviadas = True
        while not self.pronto():
            time.sleep(0.05)
        self._registrar("PRONTO", peers=sorted(self.outros_peers))

    def pronto(self):
        """Barreira local: referências e chaves de ambos, e chave própria enviada."""
        with self.lock:
            esperados = set(PEERS) - {self.nome}
            return (self._chaves_enviadas and set(self.outros_peers) == esperados
                    and set(self.chaves_publicas) == esperados)

    def _mensagem(self, tipo, recurso, destinatario, pedido_ts, pedido_id, timestamp=None):
        """Cria e assina a mensagem; chamar sob lock. Um PEDIDO é um evento lógico.

        Os dois unicasts de um pedido usam o MESMO timestamp de prioridade.
        OK/LIBERACAO têm novo timestamp de envio e ecoam a identidade do pedido.
        """
        if timestamp is None:
            self.relogio += 1
            timestamp = self.relogio
        return assinar(self._chave_privada, {
            "remetente": self.nome, "destinatario": destinatario, "tipo": tipo,
            "recurso": recurso, "timestamp": timestamp,
            "pedido_timestamp": pedido_ts, "pedido_id": pedido_id})

    def _transmitir(self, metodo, envelope):
        """Worker: nunca recebe o lock local; o Proxy pertence a esta thread."""
        m = envelope["conteudo"]
        destino = m["destinatario"]
        self._registrar("ENVIADA", mensagem=m, assinatura="ASSINADA")
        try:
            with Pyro5.api.Proxy(self.outros_peers[destino]) as outro:
                aceita = getattr(outro, metodo)(envelope)
            self._registrar("ENTREGUE", mensagem=m,
                            verificacao_destino="VALIDA" if aceita else "REJEITADA")
        except Exception as erro:
            # O enunciado exclui falhas: não conceder permissão por timeout.
            self._registrar("ERRO_ENVIO", mensagem=m, erro=str(erro))

    def _validar(self, envelope, tipo):
        """Valida ANTES de alterar relógio/filas; inválidas não afetam o algoritmo."""
        try:
            m = validar_formato(envelope, tipo, self.nome)
            with self.lock:
                chave = self.chaves_publicas.get(m["remetente"])
            if chave is None:
                raise ValueError("chave pública ainda não recebida")
            verificar(chave, envelope)
        except (InvalidSignature, ValueError, TypeError, KeyError, UnicodeError):
            # Só registra dados primitivos de diagnóstico; jamais os aplica ao estado.
            recebido = envelope.get("conteudo") if isinstance(envelope, dict) else None
            resumo = {k: v for k, v in recebido.items()
                      if k in ("remetente", "destinatario", "tipo", "recurso", "timestamp")
                      and type(v) in (str, int)} if isinstance(recebido, dict) else {}
            self._registrar("MENSAGEM_REJEITADA", assinatura="INVALIDA",
                            metodo=tipo, mensagem=resumo)
            return None
        return dict(m)

    def _recebida(self, m):
        """Evento de recepção válido: L = max(L, timestamp_remoto) + 1. Sob lock."""
        self.relogio = max(self.relogio, m["timestamp"]) + 1
        self._registrar("RECEBIDA", mensagem=m, assinatura="VALIDA")

    @Pyro5.api.expose
    def receber_pedido(self, envelope):
        """Ricart e Agrawala: responde já ou registra interesse na fila do recurso."""
        m = self._validar(envelope, "PEDIDO")
        if m is None:
            return False
        resposta = None
        with self.lock:
            self._recebida(m)
            identidade = (m["remetente"], m["recurso"])
            if m["pedido_timestamp"] <= self._ultimos_pedidos.get(identidade, 0):
                self._registrar("PEDIDO_REPETIDO_IGNORADO", pedido_id=m["pedido_id"])
                return True
            self._ultimos_pedidos[identidade] = m["pedido_timestamp"]
            r = self.recursos[m["recurso"]]
            prioridade_remota = (m["pedido_timestamp"], m["remetente"])
            prioridade_local = (r.pedido_timestamp, self.nome)
            adiar = (r.estado == "OCUPADO" or
                     (r.estado == "DESEJADO" and prioridade_local < prioridade_remota))
            if adiar:
                # Esta fila É o registro de interesse subscriber -> publisher.
                r.adiados[m["remetente"]] = m
                self._registrar("INTERESSE_REGISTRADO", recurso=m["recurso"],
                                subscriber=m["remetente"], pedido_id=m["pedido_id"],
                                pedido_timestamp=m["pedido_timestamp"])
            else:
                resposta = self._mensagem("OK", m["recurso"], m["remetente"],
                                         m["pedido_timestamp"], m["pedido_id"])
        if resposta:
            self._workers.submit(self._transmitir, "receber_ok", resposta)
        return True

    def _receber_permissao(self, envelope, tipo):
        """OK imediato e callback de liberação contam UMA permissão do remetente."""
        m = self._validar(envelope, tipo)
        if m is None:
            return False
        with self.lock:
            self._recebida(m)
            r = self.recursos[m["recurso"]]
            if r.estado != "DESEJADO" or m["pedido_id"] != r.pedido_id:
                self._registrar("PERMISSAO_FORA_DO_PEDIDO", pedido_id=m["pedido_id"])
                return True
            if m["remetente"] in r.permissoes:
                self._registrar("PERMISSAO_REPETIDA_IGNORADA", pedido_id=r.pedido_id,
                                remetente=m["remetente"])
                return True
            r.permissoes.add(m["remetente"])
            self._registrar("PERMISSAO_RECEBIDA", recurso=m["recurso"],
                            remetente=m["remetente"], tipo=tipo,
                            quantidade=len(r.permissoes), pedido_id=r.pedido_id)
            if r.permissoes == set(PEERS) - {self.nome}:
                r.estado = "OCUPADO"
                self._registrar("ENTROU_SC", recurso=m["recurso"],
                                pedido_id=r.pedido_id, pedido_timestamp=r.pedido_timestamp,
                                permissoes=sorted(r.permissoes))
        return True

    @Pyro5.api.expose
    def receber_ok(self, envelope):
        """Recebe a permissão que o outro peer pôde conceder imediatamente."""
        return self._receber_permissao(envelope, "OK")

    @Pyro5.api.expose
    def notificar(self, envelope):
        """Subscriber: LIBERACAO é o OK adiado, não uma autorização completa."""
        return self._receber_permissao(envelope, "LIBERACAO")

    def solicitar(self, recurso):
        """Inicia um pedido sem bloquear o menu enquanto aguarda as duas permissões."""
        if recurso not in RECURSOS:
            raise ValueError("use R1 ou R2")
        with self.lock:
            if not self.pronto():
                raise ValueError("aguarde as duas referências e a troca de chaves")
            r = self.recursos[recurso]
            if r.estado != "LIBERADO":
                raise ValueError(f"{recurso} já está {r.estado}")
            # Ordem global: nunca adquirir R1 enquanto pede ou mantém R2.
            if recurso == "R1" and self.recursos["R2"].estado != "LIBERADO":
                raise ValueError("política anti-impasse R1 -> R2: libere R2 antes de pedir R1")
            if recurso == "R2" and self.recursos["R1"].estado == "DESEJADO":
                raise ValueError("política anti-impasse R1 -> R2: espere R1 ficar OCUPADO antes de pedir R2")
            self.relogio += 1
            r.estado = "DESEJADO"
            r.pedido_timestamp = self.relogio
            r.pedido_id = f"{self.nome}:{recurso}:{self.relogio}"
            r.permissoes.clear()
            mensagens = [self._mensagem("PEDIDO", recurso, destino,
                                       r.pedido_timestamp, r.pedido_id,
                                       timestamp=r.pedido_timestamp)
                         for destino in sorted(self.outros_peers)]
            self._registrar("PEDIU", recurso=recurso, pedido_id=r.pedido_id,
                            pedido_timestamp=r.pedido_timestamp)
            pedido_id = r.pedido_id
        for envelope in mensagens:
            self._workers.submit(self._transmitir, "receber_pedido", envelope)
        return pedido_id

    def liberar(self, recurso):
        """Publisher: libera, remove interesses e agenda callbacks fora do lock.

        O menu retorna sem esperar os subscribers: a chamada remota síncrona
        ocorre apenas no worker. Não há broadcast extra de liberação.
        """
        if recurso not in RECURSOS:
            raise ValueError("use R1 ou R2")
        with self.lock:
            r = self.recursos[recurso]
            if r.estado != "OCUPADO":
                raise ValueError(f"{recurso} está {r.estado}; só se libera uma SC ocupada")
            self.relogio += 1
            pedido_id = r.pedido_id
            interessados = sorted(r.adiados.values(),
                                  key=lambda m: (m["pedido_timestamp"], m["remetente"]))
            r.estado = "LIBERADO"
            r.pedido_timestamp = None
            r.pedido_id = None
            r.permissoes.clear()
            r.adiados.clear()
            self._registrar("SAIU_SC", recurso=recurso, pedido_id=pedido_id,
                            notificar=[m["remetente"] for m in interessados])
            mensagens = [self._mensagem("LIBERACAO", recurso, m["remetente"],
                                       m["pedido_timestamp"], m["pedido_id"])
                         for m in interessados]
            for m in interessados:
                self._registrar("INTERESSE_REMOVIDO", recurso=recurso,
                                subscriber=m["remetente"], pedido_id=m["pedido_id"])
        for envelope in mensagens:
            self._workers.submit(self._transmitir, "notificar", envelope)

    def demonstrar_assinatura_invalida(self, destino=None, recurso="R1"):
        """Assina um pedido e altera o timestamp depois, invalidando a assinatura."""
        with self.lock:
            if not self.pronto():
                raise ValueError("peer ainda não está pronto")
            destino = destino or sorted(self.outros_peers)[0]
            if destino not in self.outros_peers or recurso not in RECURSOS:
                raise ValueError("destino/recurso inválido")
            self.relogio += 1
            ts = self.relogio
            envelope = self._mensagem("PEDIDO", recurso, destino, ts,
                                      f"{self.nome}:{recurso}:{ts}", timestamp=ts)
            # Mantém o formato coerente; SOMENTE a assinatura fica inválida.
            m = envelope["conteudo"]
            m["timestamp"] += 1
            m["pedido_timestamp"] += 1
            m["pedido_id"] = f"{self.nome}:{recurso}:{ts + 1}"
            self._registrar("ADULTERACAO_DEMONSTRACAO", destinatario=destino)
        self._workers.submit(self._transmitir, "receber_pedido", envelope)

    @Pyro5.api.expose
    def consultar_estado(self, incluir_eventos=False):
        """Snapshot somente de leitura, usado pelo menu e pela verificação externa."""
        with self.lock:
            estado = {
                "nome": self.nome, "relogio": self.relogio, "pronto": self.pronto(),
                "referencias": sorted(self.outros_peers),
                "chaves_publicas": sorted(self.chaves_publicas),
                "recursos": {nome: {
                    "estado": r.estado, "pedido_timestamp": r.pedido_timestamp,
                    "pedido_id": r.pedido_id, "permissoes": sorted(r.permissoes),
                    "fila": sorted(r.adiados.values(),
                                   key=lambda m: (m["pedido_timestamp"], m["remetente"]))}
                    for nome, r in self.recursos.items()}}
            if incluir_eventos:
                estado["eventos"] = list(self._eventos)
            return copy.deepcopy(estado)

    def pode_sair(self):
        """Evita encerrar pelo menu enquanto há SC, pedido ou interesse pendente."""
        with self.lock:
            return all(r.estado == "LIBERADO" and not r.adiados
                       for r in self.recursos.values())

    def fechar(self):
        """Espera workers terminarem e fecha o arquivo de log antes do daemon."""
        self._workers.shutdown(wait=True)
        if self._log:
            self._log.close()


def mostrar_estado(peer):
    """Mostra estados, pares de prioridade, permissões, duas filas e relógio."""
    s = peer.consultar_estado()
    print(f"\n{peer.nome}: Lamport={s['relogio']} | pronto={s['pronto']}")
    for recurso, r in s["recursos"].items():
        print(f"{recurso}: {r['estado']} | pedido=({r['pedido_timestamp']}, {peer.nome}) "
              f"| OKs={r['permissoes']}")
        print("  fila:", [(m["remetente"], m["pedido_timestamp"], m["pedido_id"])
                           for m in r["fila"]])


def menu(peer):
    """Comandos numéricos ou textuais; espera de recurso acontece em background."""
    print("\n1/pedir R1 | 2/liberar R1 | 3/pedir R2 | 4/liberar R2\n"
          "5/estado | 6/log | 7/ola | 8/nomes | 9/adulterar [PeerX] [R1|R2]\n"
          "0/sair | ajuda. Para os dois recursos: peça R1, espere OCUPADO, peça R2.",
          flush=True)
    atalhos = {"1": "pedir R1", "2": "liberar R1", "3": "pedir R2",
               "4": "liberar R2", "5": "estado", "6": "log", "7": "ola",
               "8": "nomes", "9": "adulterar", "0": "sair"}
    while True:
        try:
            linha = input(f"{peer.nome}> ").strip()
        except EOFError:
            if not peer.pode_sair():
                print("Entrada fechada com recursos ativos: execução interrompida.")
            return
        comando = atalhos.get(linha, linha).split()
        if not comando:
            continue
        try:
            op = comando[0].lower()
            if op in ("pedir", "liberar") and len(comando) == 2:
                getattr(peer, "solicitar" if op == "pedir" else "liberar")(comando[1].upper())
            elif op == "estado":
                mostrar_estado(peer)
            elif op == "log":
                for e in peer.consultar_estado(True)["eventos"][-30:]:
                    print(json.dumps(e, ensure_ascii=False))
            elif op == "adulterar" and len(comando) <= 3:
                peer.demonstrar_assinatura_invalida(
                    comando[1] if len(comando) > 1 else None,
                    comando[2].upper() if len(comando) > 2 else "R1")
            elif op == "ola":
                for uri in peer.outros_peers.values():
                    with Pyro5.api.Proxy(uri) as outro:
                        outro.ola(peer.nome)
            elif op == "nomes":
                with Pyro5.api.locate_ns(host=sn.HOST_NS, port=sn.PORTA_NS) as ns:
                    print("Registrados:", sorted(ns.list()))
            elif op == "sair":
                if peer.pode_sair():
                    return
                print("Saída recusada: aguarde seus pedidos e libere os recursos ativos.")
            else:
                print("Use pedir/liberar R1/R2, estado, log, ola, nomes, adulterar, sair.")
        except (ValueError, Pyro5.errors.PyroError) as erro:
            peer._registrar("COMANDO_RECUSADO", comando=linha, motivo=str(erro))


def main():
    """Preserva serviço de nomes único e lookup; acrescenta chaves antes do menu."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("nome", choices=PEERS)
    parser.add_argument("--porta-ns", type=int, default=9090,
                        help="use a mesma porta nos três peers (padrão: 9090)")
    parser.add_argument("--logs", default="logs", help="pasta dos logs JSONL")
    args = parser.parse_args()
    sn.PORTA_NS = args.porta_ns
    print(f"[{args.nome}] PID deste processo: {os.getpid()}", flush=True)
    ns = sn.obter_ou_criar_servico_de_nomes(args.nome)
    peer = Peer(args.nome, Path(args.logs) / f"{args.nome}.jsonl")
    daemon = Pyro5.api.Daemon(host=sn.HOST_NS)
    registrado = False
    try:
        uri = daemon.register(peer)
        # safe=True evita sobrescrever um peer de outra execução na mesma porta.
        ns.register(args.nome, uri, safe=True)
        registrado = True
        threading.Thread(target=daemon.requestLoop, daemon=True).start()
        print(f"[{args.nome}] registrado no serviço de nomes: {uri}", flush=True)
        for outro in PEERS:
            if outro != args.nome:
                peer.outros_peers[outro] = sn.aguardar_peer(ns, outro, args.nome)
                print(f"[{args.nome}] referência de {outro} obtida.", flush=True)
        peer.trocar_chaves()
        menu(peer)
    except KeyboardInterrupt:
        print("\nExecução interrompida. O trabalho pressupõe ausência de falhas.")
    finally:
        peer.fechar()
        if registrado:
            try:
                ns.remove(args.nome)
            except Pyro5.errors.PyroError:
                pass
        ns._pyroRelease()
        daemon.shutdown()
        daemon.close()
        print(f"[{args.nome}] encerrado.", flush=True)


if __name__ == "__main__":
    main()
