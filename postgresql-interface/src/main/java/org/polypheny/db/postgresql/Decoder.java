package org.polypheny.db.postgresql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.polypheny.db.StatusService;

import java.util.List;

public class Decoder extends LengthFieldBasedFrameDecoder {

    private static final int HEADER_SIZE = 1;
    private byte type;
    private int length;
    private String msgBody;
    private ChannelHandlerContext ctx;
    private ByteBuf in;
    private List<Object> out;


    public Decoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip) throws Exception {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength,
                lengthAdjustment, initialBytesToStrip);
        Object decoded = decode(ctx, in);
        ctx.write(decoded);
    }


    /*
    @Override
    protected void decode (ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        this.ctx = ctx;
        this.in = in;
        Object decoded = decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }

    }

    @Override
    protected Object decode (ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in == null) {
            return null;
        }
        if (in.readableBytes() < HEADER_SIZE) {
            throw new Exception("Only Header");
        }


        type = in.readByte();
        length = in.readByte();

        if(in.readableBytes() < length) {
            //throw new Exception("The message is too short");
        }

        ByteBuf buf = in.readBytes(length);
        byte[] b = new byte[buf.readableBytes()];
        buf.readBytes(b);

        msgBody = new String(b, "UTF-8");
        Message msg = new Message(type, length, msgBody);

        //StatusService.printInfo(String.format("decoded message:" + msgBody));

        return msg;


    }

     */
}
