package com.smattme.cassandra2sql.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.util.Properties;

/**
 * Created by seun_ on 25-Feb-18.
 *
 */
public class EmailService {

    private String host = "";
    private int port = 0;
    private String fromAdd = "";
    private String toAdd = "";
    private String username = "";
    private String password = "";
    private String subject = "";
    private String msg = "";
    private File [] attachments;
    private Logger logger = LoggerFactory.getLogger(EmailService.class);


    private EmailService() {}

    /**
     * This is used to instantiate this class and form a
     * builder pattern
     * @return EmailService a new instance of this class
     */
    public static EmailService builder() {
        return new EmailService();
    }

    public EmailService setHost(String host){
        this.host = host;
        return this;
    }

    public EmailService setPort(int port) {
        this.port = port;
        return this;
    }

    public EmailService setFromAddress(String fromAdd) {
        this.fromAdd = fromAdd;
        return this;
    }

    public EmailService setToAddress(String toAdd) {
        this.toAdd = toAdd;
        return  this;
    }

    public EmailService setUsername(String username) {
        this.username = username;
        return this;
    }

    public EmailService setPassword(String password) {
        this.password = password;
        return this;
    }

    public EmailService setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public EmailService setMessage(String message) {
        this.msg = message;
        return this;
    }

    public EmailService setAttachments(File[] files) {
        this.attachments = files;
        return this;
    }

    /**
     * This will check if the necessary properties
     * are set for sending an email successfully
     * @return boolean
     */
    private boolean isPropertiesSet() {
        return !this.host.isEmpty() &&
                this.port > 0 &&
                !this.username.isEmpty() &&
                !this.password.isEmpty() &&
                !this.toAdd.isEmpty() &&
                !this.fromAdd.isEmpty() &&
                !this.subject.isEmpty() &&
                !this.msg.isEmpty() &&
                this.attachments != null && this.attachments.length > 0;
    }


    /**
     * This function will send an email
     * and add the generated sql file as an attachment
     * @return boolean
     */
    public boolean sendMail() {

        if(!this.isPropertiesSet()) {
            logger.debug(" Required Mail Properties are not set. Attachments will not be sent");
            return false;
        }

        Properties prop = new Properties();
        prop.put("mail.smtp.auth", true);
        prop.put("mail.smtp.starttls.enable", "true");
        prop.put("mail.smtp.host", this.host);
        prop.put("mail.smtp.port", this.port);
        prop.put("mail.smtp.ssl.trust", host);

        logger.debug(" Mail properties set");

        Session session = Session.getInstance(prop, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        logger.debug(" Mail Session Created");

        try {
//			create a default mime message object
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(fromAdd));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toAdd));
            message.setSubject(subject);

//          body part for message
            MimeBodyPart mimeBodyPart = new MimeBodyPart();
            mimeBodyPart.setContent(msg, "text/html");

//          body part for attachments
            MimeBodyPart attachmentBodyPart = new MimeBodyPart();
            logger.debug(" " + this.attachments.length + " attachments found");
            for (File file: this.attachments) {
                attachmentBodyPart.attachFile(file);
            }

//          create a multipart to combine them together
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(mimeBodyPart);
            multipart.addBodyPart(attachmentBodyPart);

            //now set the multipart as the content of the message
            message.setContent(multipart);

//			send the message
            Transport.send(message);

            logger.debug(" MESSAGE SENT SUCCESSFULLY");

            return true;

        } catch (Exception e) {
            logger.debug("MESSAGE NOT SENT. " + e.getLocalizedMessage(), e);
            return false;
        }

    }

}
