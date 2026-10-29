package peer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classe servente do peer: implementa a interface remota e guarda o estado
 * local do processo.
 *
 * ATENÇÃO À CONCORRÊNCIA: o Java RMI atende cada chamada remota numa thread
 * própria. Enquanto o usuário mexe no menu (thread main), outros peers podem
 * estar chamando ola()/notificar() ao mesmo tempo. Todo estado compartilhado
 * (filas, relógio lógico, estado dos recursos...) precisa ser protegido
 * (synchronized, java.util.concurrent etc.).
 */
public class PeerImpl extends UnicastRemoteObject implements InterfacePeer {

    private final String nome;

    /** Referências de objeto remoto dos OUTROS peers (nome -> referência). */
    private final Map<String, InterfacePeer> outrosPeers = new ConcurrentHashMap<>();

    // TODO (T1): estado do algoritmo de Ricart e Agrawala, por recurso
    //   - estado de cada recurso (LIBERADO / DESEJADO / OCUPADO);
    //   - relógio lógico de Lamport e timestamp do pedido pendente;
    //   - uma fila por recurso com os pedidos adiados (registros de interesse);
    //   - quantas permissões (OK) já chegaram para o pedido atual.
    // TODO (T1): par de chaves deste peer e chaves públicas dos outros peers.

    public PeerImpl(String nome) throws RemoteException {
        super(); // exporta o objeto: a partir daqui ele já pode receber chamadas
        this.nome = nome;
    }

    /** Guarda a referência remota de outro peer (chamado pelo Peer.main após o lookup). */
    public void adicionarPeer(String nomePeer, InterfacePeer referencia) {
        outrosPeers.put(nomePeer, referencia);
    }

    public Map<String, InterfacePeer> getOutrosPeers() {
        return outrosPeers;
    }

    // ------------------------------------------------------------------
    // Métodos remotos (chamados POR OUTROS peers)
    // ------------------------------------------------------------------

    @Override
    public String getNome() throws RemoteException {
        return nome;
    }

    @Override
    public void ola(String remetente) throws RemoteException {
        System.out.println("\n[" + nome + "] recebeu olá de " + remetente);
    }

    @Override
    public void notificar(String remetente, String mensagem) throws RemoteException {
        // TODO (T1): validar a assinatura digital e tratar o evento recebido
        // (ex.: contabilizar a permissão para entrar na SC do recurso).
        System.out.println("\n[" + nome + "] notificação de " + remetente + ": " + mensagem);
    }

    // ------------------------------------------------------------------
    // Métodos locais (chamados pelo menu deste próprio peer)
    // ------------------------------------------------------------------

    /**
     * Envia "olá" para cada um dos outros peers, um por vez (unicast).
     * Repare que é uma chamada SÍNCRONA: a thread fica bloqueada até o outro
     * peer terminar de executar ola().
     *
     * TODO (T1): o enunciado pede que a notificação de evento seja ASSÍNCRONA —
     * dispare a chamada a notificar() numa thread separada (ex.: new Thread,
     * ExecutorService ou CompletableFuture.runAsync) para que o publisher não
     * fique bloqueado esperando o subscriber.
     */
    public void enviarOlaParaTodos() {
        for (Map.Entry<String, InterfacePeer> e : outrosPeers.entrySet()) {
            try {
                e.getValue().ola(nome);
                System.out.println("[" + nome + "] olá entregue a " + e.getKey());
            } catch (RemoteException ex) {
                System.out.println("[" + nome + "] falha ao falar com " + e.getKey() + ": " + ex.getMessage());
            }
        }
    }
}
