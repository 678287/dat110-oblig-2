package no.hvl.dat110.broker;

import java.util.Set;
import java.util.Collection;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

    private Storage storage;

    public Dispatcher(Storage storage) {
        super("Dispatcher");
        this.storage = storage;
    }

    @Override
    public void doProcess() {

        Collection<ClientSession> clients = storage.getSessions();

        Logger.lg(".");
        for (ClientSession client : clients) {

            Message msg = null;

            if (client.hasData()) {
                msg = client.receive();
            }

            // a message was received
            if (msg != null) {
                dispatch(client, msg);
            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void dispatch(ClientSession client, Message msg) {

        MessageType type = msg.getType();

        // invoke the appropriate handler method
        switch (type) {

        case DISCONNECT:
            onDisconnect((DisconnectMsg) msg);
            break;

        case CREATETOPIC:
            onCreateTopic((CreateTopicMsg) msg);
            break;

        case DELETETOPIC:
            onDeleteTopic((DeleteTopicMsg) msg);
            break;

        case SUBSCRIBE:
            onSubscribe((SubscribeMsg) msg);
            break;

        case UNSUBSCRIBE:
            onUnsubscribe((UnsubscribeMsg) msg);
            break;

        case PUBLISH:
            onPublish((PublishMsg) msg);
            break;

        default:
            Logger.log("broker dispatch - unhandled message type");
            break;

        }
    }

    // called from Broker after having established the underlying connection
    public void onConnect(ConnectMsg msg, Connection connection) {

        String user = msg.getUser();

        Logger.log("onConnect:" + msg.toString());

        storage.addClientSession(user, connection);

    }

    // called by dispatch upon receiving a disconnect message
    public void onDisconnect(DisconnectMsg msg) {

        String user = msg.getUser();

        Logger.log("onDisconnect:" + msg.toString());

        storage.removeClientSession(user);

    }

    public void onCreateTopic(CreateTopicMsg msg) {

        Logger.log("onCreateTopic:" + msg.toString());

        // create the topic in the broker storage
        // the topic is contained in the create topic message
        storage.createTopic(msg.getTopic());
    }

    public void onDeleteTopic(DeleteTopicMsg msg) {

        Logger.log("onDeleteTopic:" + msg.toString());

        // delete the topic from the broker storage
        // the topic is contained in the delete topic message
        storage.deleteTopic(msg.getTopic());
    }

    public void onSubscribe(SubscribeMsg msg) {

        Logger.log("onSubscribe:" + msg.toString());

        // subscribe user to the topic
        // user and topic is contained in the subscribe message
        storage.addSubscriber(msg.getUser(), msg.getTopic());
    }

    public void onUnsubscribe(UnsubscribeMsg msg) {

        Logger.log("onUnsubscribe:" + msg.toString());

        // unsubscribe user from the topic
        // user and topic is contained in the unsubscribe message
        storage.removeSubscriber(msg.getUser(), msg.getTopic());
    }

    public void onPublish(PublishMsg msg) {

        Logger.log("onPublish:" + msg.toString());

        // publish the message to clients subscribed to the topic
        // topic and message is contained in the publish message
        // messages must be sent using the corresponding client session objects
        Set<String> subscribers = storage.getSubscribers(msg.getTopic());
        if (subscribers != null) {
            for (String user : subscribers) {
                ClientSession session = storage.getSession(user);
                if (session != null) {
                    session.send(msg);
                }
            }
        }
    }
}