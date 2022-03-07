package com.qdspr.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class AmqpTest {

	
	public void test() throws IOException, TimeoutException {
		// 1.创建连接工厂
		ConnectionFactory factory = new ConnectionFactory();// MQ采用工厂模式来完成连接的创建
		// 2.在工厂对象中设置连接信息(ip,port,virtualhost,username,password)
		factory.setHost("localhost");// 设置MQ安装的服务器ip地址
		factory.setPort(5672);// 设置端口号
		factory.setVirtualHost("test");// 设置虚拟主机名称
		// MQ通过用户来管理
		factory.setUsername("admin");// 设置用户名称
		factory.setPassword("admin");// 设置用户密码
		// 3.通过工厂对象获取连接
		Connection connection = factory.newConnection();

		// 创建Channel
		Channel channel = connection.createChannel();
		String msg = "hello world";
		// basicPublish将消息发送到指定的交换机
		channel.basicPublish("ex3", "a", null, msg.getBytes());
		// 关闭连接
		channel.close();
		connection.close();
	}

}
