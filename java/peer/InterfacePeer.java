package peer;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface remota ÚNICA de um processo par (peer).
 *
 * Numa arquitetura de processos pares não existe "interface do servidor" e
 * "interface do cliente": todo peer expõe a mesma interface e todo peer chama
 * os métodos dos outros. Por isso há um só arquivo de interface, compartilhado
 * por PeerA, PeerB e PeerC.
 *
 * Todo método remoto precisa declarar RemoteException.
 *
 * TODO (T1): acrescente aqui os métodos remotos de que o seu protocolo precisa,
 * por exemplo:
 *   - receber a chave pública de outro peer;
 *   - receber um pedido de acesso a um recurso (com timestamp de Lamport e
 *     assinatura digital);
 *   - receber a resposta/permissão (OK) de outro peer.
 */
public interface InterfacePeer extends Remote {

    /** Devolve o nome com que este peer se registrou no serviço de nomes. */
    String getNome() throws RemoteException;

    /**
     * Mensagem de teste: outro peer diz "olá". Serve só para confirmar que a
     * comunicação unicast entre os três processos está funcionando.
     */
    void ola(String remetente) throws RemoteException;

    /**
     * Método para RECEBER notificações de eventos (papel de subscriber).
     * O enunciado do T1 exige que todo peer tenha um método deste tipo.
     *
     * TODO (T1): defina o conteúdo da notificação (recurso liberado, timestamp,
     * assinatura...) — troque os parâmetros se preferir um objeto Serializable.
     */
    void notificar(String remetente, String mensagem) throws RemoteException;
}
