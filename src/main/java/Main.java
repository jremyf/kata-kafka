public class Main {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("arguments must be [action] [topic] ([message])");
            System.err.println("ProducerExample : produce topic_userX Hello!");
            System.err.println("ConsumerExample : consume topic_userX");
        }
        String action = args[0];
        String topic = args[1];

        switch (action) {
            case "consume":
                ConsumerExample.receiveMsg(topic);
                break;
            case "produce":
                if (args.length < 3) {
                    System.err.println("message to send missing");
                }
                String message = args[2];
                ProducerExample.sendMsg(topic, message);
                break;
            default:
                System.err.println("wrong action");
        }
    }
}
