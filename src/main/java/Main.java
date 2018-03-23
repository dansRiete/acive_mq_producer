import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;

import javax.jms.*;

/**
 * Hello world!
 */
public class Main {

    public static void main(String[] args) throws Exception {
        thread(new HelloWorldProducer(), false);/*
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        Thread.sleep(1000);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        Thread.sleep(1000);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldProducer(), false);
        Thread.sleep(1000);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldConsumer(), false);
        thread(new HelloWorldProducer(), false);*/
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.99.100:32768");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic("MQ.REPORT_BUILDER.REPORTS.TOPIC");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                TextMessage message = session.createTextMessage(text);
                BytesMessage bytesMessage = new ActiveMQBytesMessage();
                bytesMessage.setJMSType(null);
                bytesMessage.setJMSDeliveryMode(2);
                bytesMessage.setJMSExpiration(0);
                bytesMessage.setJMSPriority(4);
                bytesMessage.setJMSMessageID("ID:414d512061757330382020202020202080e3685a48e79021");
                bytesMessage.setJMSCorrelationID("ID:414d512061757330382020202020202080e3685a051e5e20");
                bytesMessage.setJMSRedelivered(false);
                bytesMessage.writeBytes(
                        ("    CM_CLIENT_ID: 30000001\n"
                        + "    CM_DATE_ID: 20180215\n"
                        + "    CM_REPORT_ID: 1111\n"
                        + "    CM_REPORT_TYPE: DAILY\n"
                        + "    JMSXAppID: aus08                       \n"
                        + "    JMSXDeliveryCount: 1\n"
                        + "    JMSXUserID: unagi       \n"
                        + "    JMS_IBM_Character_Set: UTF-8\n"
                        + "    JMS_IBM_ConnectionID: 414D514361757330382020202020202080E3685A02E69020\n"
                        + "    JMS_IBM_Encoding: 273\n"
                        + "    JMS_IBM_Format:         \n"
                        + "    JMS_IBM_MsgType: 8\n"
                        + "    JMS_IBM_PutApplType: 26\n"
                        + "    JMS_IBM_PutDate: 20180302\n"
                        + "    JMS_IBM_PutTime: 15254965\n"
                        + "1f8b08000000000000007d8db10ac24010443787a984888df83b671ce2a2d93b6f3749b9d80b36fe\n"
                        + "3f1a4c21160e3c187803d310511da83abee9bf693e62dbc77286b174de9ea2082e813683f075808f\n"
                        + "ac6ca968a03d4688794e2ca69e515ca1ca4902ed72ece629a61fb13ea4415a788986f9ac5a1e6b7b\n"
                        + "3c6ff7152df9535efbc52a00bc000000").getBytes()
                );

                // Tell the producer to send the message
                System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
                BytesMessage bytesMessage1 = session.createBytesMessage();
                bytesMessage1.setJMSType(null);
                bytesMessage1.setJMSDeliveryMode(2);
                bytesMessage1.setJMSExpiration(0);
                bytesMessage1.setJMSPriority(4);
                bytesMessage1.setJMSMessageID("ID:414d512061757330382020202020202080e3685a48e79021");
                bytesMessage1.setJMSCorrelationID("ID:414d512061757330382020202020202080e3685a051e5e20");
                bytesMessage1.setJMSRedelivered(false);
                bytesMessage1.setStringProperty("CM_CLIENT_ID", "30003001");
                bytesMessage1.setStringProperty("CM_DATE_ID", "20180313");
                bytesMessage1.setStringProperty("CM_REPORT_ID", "1111");
                bytesMessage1.setStringProperty("CM_REPORT_TYPE", "daily");
                bytesMessage1.setStringProperty("JMSXAppID", "aus08");
                bytesMessage1.setStringProperty("JMSXDeliveryCount", "1");
                bytesMessage1.setStringProperty("JMSXUserID", "unagi");
                bytesMessage1.setStringProperty("JMS_IBM_Character_Set", "UTF-8");
                bytesMessage1.setStringProperty("JMS_IBM_ConnectionID", "414D514361757330382020202020202080E3685A02E69020");
                bytesMessage1.setStringProperty("JMS_IBM_Encoding", "273");
                bytesMessage1.setStringProperty("JMS_IBM_Format", "");
                bytesMessage1.setStringProperty("JMS_IBM_MsgType", "8");
                bytesMessage1.setStringProperty("JMS_IBM_PutApplType", "26");
                bytesMessage1.setStringProperty("JMS_IBM_PutDate", "20180302");
                bytesMessage1.setStringProperty("JMS_IBM_PutTime", "15254965");
                StringBuilder stringBuilder = new StringBuilder();
                for(int i = 0; i < 1000; i++){
                    stringBuilder.append("1f8b08000000000000007d8db10ac24010443787a984888df83b671ce2a2d93b6f3749b9d80b36fe");
                }
                bytesMessage1.writeBytes(
                        stringBuilder.toString().getBytes()
                );

                producer.send(bytesMessage1);

                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {

                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.99.100:32774");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createTopic("MQ.REPORT_BUILDER.REPORTS.TOPIC");

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                // Wait for a message
                Message message = consumer.receive(1000);

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Received: " + text);
                } else {
                    System.out.println("Received: " + message);
                }

                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}