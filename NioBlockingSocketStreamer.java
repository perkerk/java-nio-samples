/**
 * Handler that allows for input and output streams of an NIO socket to be
 * read as blocking I/O
 * <p>
 * Reading and writing from the streams <b>must</b> be done on the same thread
 * @author Eric
 *
 */
public class NioBlockingSocketStreamer
{
    private static long NIO_SELECT_TIMEOUT = 0;
    private static long NIO_WRITE_TIMEOUT_SECONDS = 30;
    /** poison pill result that signals a disconnected channel */
    private static int NIO_READ_IOEXCEPTION = -2;

    private final SocketChannel socketChannel;

    private Selector selector;

    private final InputStream inputStream = new MyInputStream(this);
    private final OutputStream outputStream = new MyOutputStream(this);

    private final LinkedBlockingQueue<ByteBuffer> pendingWrites;
    private final MyReadQueue<ReadRequest> pendingRead;

    private volatile boolean disconnected = false;

    public NioBlockingSocketStreamer(SocketChannel socketChannel,
                                     int writeQueueSize)
    {
        pendingRead = new MyReadQueue<ReadRequest>( );
        pendingWrites = new LinkedBlockingQueue<ByteBuffer>(writeQueueSize);
        this.socketChannel = socketChannel;
    }

    public NioBlockingSocketStreamer start( )
            throws IOException
    {
        this.selector = Selector.open( );
        this.socketChannel.configureBlocking(false);

        final NioBlockingSocketStreamer streamer = this;
        Thread t = new Thread(new Runnable( )
        {
            @Override
            public void run( )
            {
                try
                {
                    streamer.nioServiceLoop( );
                }
                catch (ClosedChannelException e)
                {
                    //shutting down
                }
                catch (ClosedSelectorException e)
                {
                    //shutting down
                }
                catch (IOException e)
                {
                    e.printStackTrace( );
                }
            }
        });
        t.start( );
        return this;
    }

    public void nioServiceLoop( )
            throws IOException
    {
        ByteBuffer writeBuffer = null;
        ReadRequest readRequest = null;

        try
        {
            for (;;)
            {
                if (readRequest == null)
                    readRequest = pendingRead.poll( );
                int interestOps = readRequest != null
                        ? SelectionKey.OP_READ
                        : 0;
                if (writeBuffer == null && pendingWrites.isEmpty( ))
                    interestOps &= ~SelectionKey.OP_WRITE;
                else
                    interestOps |= SelectionKey.OP_WRITE;
                socketChannel.register(selector, interestOps);

                int n = selector.select(NIO_SELECT_TIMEOUT);
                if (n > 0)
                {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys( );
                    for (SelectionKey selectedKey : selectedKeys)
                    {
                        if (selectedKey.isValid( ))
                        {
                            if (selectedKey.isReadable( ))
                            {
                                int cbRead;
                                try
                                {
                                    cbRead = socketChannel.read(readRequest.buffer);
                                }
                                catch (IOException e)
                                {
                                    //"java.io.IOException: An existing connection was forcibly closed by the remote host"
                                    //is the typical disconnection
                                    return;
                                }
                                interestOps &= ~SelectionKey.OP_READ;
                                readRequest.submitResult(cbRead);
                                readRequest = null;
                            }

                            if (selectedKey.isWritable( ))
                            {
                                if (writeBuffer == null)
                                    writeBuffer = pendingWrites.poll( );

                                if (writeBuffer != null)
                                {
                                    try
                                    {
                                        socketChannel.write(writeBuffer);
                                    }
                                    catch (IOException e)
                                    {
                                        //"java.io.IOException: An existing connection was forcibly closed by the remote host"
                                        //is the typical disconnection
                                        return;
                                    }
                                    if (!writeBuffer.hasRemaining( ))
                                        writeBuffer = null;
                                }
                            }
                        }
                    }
                    selectedKeys.clear( );
                }
            }
        }
        finally
        {
            disconnected = true;

            //submit poison pill result for outstanding read request
            if (readRequest != null)
            {
                readRequest.submitResult(NIO_READ_IOEXCEPTION);
            }

            //since the read request queue became empty before we disconnected, check for the next
            //read request that may have queued in the meantime
            readRequest = pendingRead.poll( );
            if (readRequest != null)
            {
                readRequest.submitResult(NIO_READ_IOEXCEPTION);
            }
        }
    }

    private ReadRequest readRequest = new ReadRequest( );

    public synchronized int read(ByteBuffer buffer)
            throws IOException
    {
        if (disconnected)
            throw new IOException("disconnected");

        readRequest.buffer = buffer;
        int result;
        try
        {
            pendingRead.offer(readRequest);
            selector.wakeup( );
            result = readRequest.getResult( );
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }

        if (result == NIO_READ_IOEXCEPTION)
            throw new IOException("disconnected");

        return result;
    }

    public void write(ByteBuffer buffer)
            throws IOException
    {
        if (disconnected)
            throw new IOException("disconnected");

        boolean queued = false;
        if (pendingWrites.remainingCapacity( ) > 0)
        {
            try
            {
                queued = pendingWrites.add(buffer);
            }
            catch (IllegalStateException e)
            {
                throw new IOException("write queue capacity exceeded");
            }
        }
        else
        {
            try
            {
                queued = pendingWrites.offer(buffer, NIO_WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                throw new IOException(e);
            }
        }

        if (!queued)
            throw new IOException("unable to queue write");

        if (selector != null) //selector should never be null
        {
            selector.wakeup( );
        }
    }

    public InputStream getInputStream( )
    {
        return inputStream;
    }

    public OutputStream getOutputStream( )
    {
        return outputStream;
    }

    /**
     * Implements a simple blocking mechanism for getting the results of a read
     * <p>
     * <b>Note</b> This class must avoid autoboxing due to Retroweaver
     */
    static class ReadRequest
    {
        ByteBuffer buffer;
        LinkedBlockingQueue<Integer> result = new LinkedBlockingQueue<Integer>(1);

        void submitResult(int cb)
        {
            result.add(new Integer(cb));
        }

        int getResult( )
                throws InterruptedException
        {
            return (int) result.take( );
        }
    }

    static class MyInputStream
        extends InputStream
    {

        private final NioBlockingSocketStreamer streamer;

        MyInputStream(NioBlockingSocketStreamer streamer)
        {
            this.streamer = streamer;
        }

        @Override
        public int read(byte[] b,
                        int off,
                        int len)
                throws IOException
        {
            ByteBuffer buffer = ByteBuffer.allocate(len);
            int n = streamer.read(buffer);
            if (n > 0)
                System.arraycopy(buffer.array( ), 0, b, off, n);
            return n;
        }

        @Override
        public int read(byte[] b)
                throws IOException
        {
            return read(b, 0, b.length);
        }

        @Override
        public int read( )
                throws IOException
        {
            return read(new byte[1], 0, 1);
        }

        @Override
        public void close( )
                throws IOException
        {
            // TODO Auto-generated method stub
        }

    }

    static class MyOutputStream
        extends OutputStream
    {

        private final NioBlockingSocketStreamer streamer;

        MyOutputStream(NioBlockingSocketStreamer streamer)
        {
            this.streamer = streamer;
        }

        @Override
        public void write(byte[] b,
                          int off,
                          int len)
                throws IOException
        {
            ByteBuffer buffer = ByteBuffer.allocate(len);
            buffer.put(b, off, len);
            buffer.flip( );
            streamer.write(buffer);
        }

        @Override
        public void write(byte[] b)
                throws IOException
        {
            write(b, 0, b.length);
        }

        @Override
        public void write(int b)
                throws IOException
        {
            write(new byte[1], 0, 1);
        }

        @Override
        public void close( )
                throws IOException
        {
            // TODO Auto-generated method stub
        }

        @Override
        public void flush( )
                throws IOException
        {
            // TODO Auto-generated method stub
        }
    }

    static class MyReadQueue<T>
    {
        //FIXME using LinkedBlockingQueue is overkill
        private final LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<T>(1);

        public T poll( )
        {
            return queue.poll( );
        }

        public void offer(T request)
                throws InterruptedException
        {
            queue.offer(request, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
    }
}