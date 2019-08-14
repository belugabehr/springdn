package io.github.belugabehr.datanode.comms.netty.dt;

import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * A decoder that splits the received ByteBufs dynamically into HDFS pipeline
 * packets. Each HDFS packet looks like:
 *
 * <table border="1">
 * <tr>
 * <td>PLEN</td>
 * <td>HLEN</td>
 * <td>HEADER</td>
 * <td>CHECKSUMS</td>
 * <td>DATA</td>
 * </tr>
 * <tr>
 * <td>32-bit</td>
 * <td>16-bit</td>
 * <td>protobuf</td>
 * <td>byte array</td>
 * <td>byte array</td>
 * </tr>
 * </table>
 * <ul>
 * <li>Payload Length (PLEN) = length(PLEN) + length(CHECKSUMS) + length(DATA)
 * This length includes its own encoded length in the sum for historical
 * reasons.</li>
 * <li>Header Length (HLEN) = length(HEADER)</li>
 * <li>HEADER: the actual packet header fields, encoded in protobuf</li>
 * <li>CHECKSUMS: the CRCs for the data chunk. May be missing if checksums were
 * not requested DATA the actual block data</li>
 * </ul>
 * <p>
 * The length of each frame can therefore be calculated as:
 * </p>
 * <tt>PLEN<SUB>HDFS</SUB> = PLEN + LENGTH(HLEN) + HLEN</tt>
 */
final class HdfsPacketFrameDecoder extends LengthFieldBasedFrameDecoder {

  private final Logger LOG = LoggerFactory.getLogger(HdfsPacketFrameDecoder.class);
  private static final int MAX_PACKET_SIZE = 16 * 1024 * 1024;

  public HdfsPacketFrameDecoder() {
    // The first 6 bytes contain the two size fields, subtract 4 bytes since
    // PLEN includes its own size (for historical reasons).
    super(MAX_PACKET_SIZE, 0, 6, -4, 0);
  }

  @Override
  protected long getUnadjustedFrameLength(final ByteBuf buf, final int offset, final int length,
      final ByteOrder order) {
    @SuppressWarnings("deprecation")
    final ByteBuf orderedBuf = buf.order(order);
    final long packetLength = orderedBuf.getUnsignedInt(offset) + orderedBuf.getUnsignedShort(offset + 4);
    LOG.debug("HDFS Packet Length: {}b", packetLength);
    return packetLength;
  }

}
