# java-nio-samples

NioBlockingSocketStreamer.java is an adapter for using Java NIO to read and write to SocketStreams synchronously the way regular blocking I/O calls would write to Sockets. I had to do this because we use the Jsch project and had thousands of threads listening on different ports. This allows a single listener thread to listen for connections, launching a new thread for each connection, and existing code written for sockets can read and write using this class instead.
