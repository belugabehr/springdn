package io.github.belugabehr.datanode.comms.netty.dt;

import java.nio.channels.FileChannel;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ChecksumTypeProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.cloudera.datanode.domain.DataNodeDomain.ChecksumInfo;
import com.google.common.primitives.Ints;

import io.github.belugabehr.datanode.storage.block.BlockManager;
import io.github.belugabehr.datanode.util.BlockPacketWriter;
import io.github.belugabehr.datanode.util.ChunkedPacketStream;
import io.github.belugabehr.datanode.util.ProtobufEncoder;
import io.github.belugabehr.datanode.util.ProtobufVarint32LengthField;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class OpReadBlockChannelInboundHandler extends DefaultSimpleChannelInboundHandler<OpReadBlockProto> {

  private static final Logger LOG = LoggerFactory.getLogger(OpReadBlockChannelInboundHandler.class);

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final OpReadBlockProto op) throws Exception {
    LOG.debug("{}", op);

    // Checksum - Payload
    final BlockManager blockManager = getStorageManager(ctx);
    final Pair<BlockMetaData, FileChannel> metaAndPayload = blockManager.getBlock(op);
    final ChecksumInfo checksumInfo = blockManager.getBlockChecksum(metaAndPayload.getKey().getBlockId());

    // TODO: Validate offset+length is less than block length

    final FileChannel bfc = metaAndPayload.getRight();

    final BlockPacketWriter packetWriter = BlockPacketWriter.newBuilder().source(bfc)
        .checksums(checksumInfo.getChecksumChunks().toByteArray())
        .checksumChunkSize(checksumInfo.getChecksumChunkSize()).targetPayloadSize(8 * (8192 + Ints.BYTES))
        .readOffset(Math.toIntExact(op.getOffset())).readLength(Math.toIntExact(op.getLen())).build();

    final BlockOpResponseProto response = BlockOpResponseProto
        .newBuilder().setStatus(Status.SUCCESS).setReadOpChecksumInfo(ReadOpChecksumInfoProto.newBuilder()
            .setChunkOffset(packetWriter.getFilePosition()).setChecksum(ChecksumProto.newBuilder()
                .setBytesPerChecksum(checksumInfo.getChecksumChunkSize()).setType(ChecksumTypeProto.CHECKSUM_CRC32C).build())
            .build())
        .build();

    final ByteBuf protoByteBuf = ProtobufEncoder.encode(response);
    final ByteBuf protoLength = ProtobufVarint32LengthField.getLength(protoByteBuf);
    ctx.pipeline().firstContext().write(protoLength);
    ctx.pipeline().firstContext().writeAndFlush(protoByteBuf);

    ctx.writeAndFlush(new ChunkedPacketStream(packetWriter)).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception caught while processing ReadBlock op", cause);
    ctx.close();
  }
}
