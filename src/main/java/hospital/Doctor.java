package hospital;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static hospital.ExaminationType.*;


public class Doctor {
    private final static String EXAMINATION_EXCHANGE = "examination";
    private final static String RESULTS_EXCHANGE = "results";


    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();
             Scanner sc = new Scanner(System.in)) {
            channel.exchangeDeclare(EXAMINATION_EXCHANGE, "topic");


            while (sc.hasNextLine()) {
                String examinationJob = sc.nextLine();
                String[] examinationArgs = examinationJob.split(" ");

                if (examinationArgs.length != 2) {
                    System.out.println("Unknown command");
                    continue;
                }

                String examinationTypeStr = examinationArgs[0];
                String patientName = examinationArgs[1];

                ExaminationType examinationType;
                try {
                    examinationType = ExaminationType.valueOf(examinationTypeStr.toUpperCase());
                } catch (IllegalArgumentException e) {
                    System.out.println("Invalid examination type: " + examinationTypeStr);
                    continue;
                }

                orderExamination(examinationType, channel, patientName);


            }


        }
    }

    private static void orderExamination(ExaminationType examinationType, Channel channel, String patientName) throws IOException {
        switch (examinationType) {
            case KNEE:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE, 
                        KNEE.name(),
                        null, 
                        patientName.getBytes(StandardCharsets.UTF_8)
                );
                break;
            case HIP:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE,
                        HIP.name(),
                        null, 
                        patientName.getBytes(StandardCharsets.UTF_8)
                );
                break;
            case ELBOW:
                channel.basicPublish(
                        EXAMINATION_EXCHANGE,
                        ELBOW.name(),
                        null,
                        patientName.getBytes(StandardCharsets.UTF_8)
                );
                break;
        }
    }


}
