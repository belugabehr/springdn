package io.github.belugabehr.datanode.comms.netty.dt;

import com.google.common.base.Preconditions;

import io.github.belugabehr.datanode.storage.BlockManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;

abstract class DefaultSimpleChannelInboundHandler<E> extends SimpleChannelInboundHandler<E> {
	
	protected BlockManager getStorageManager(final ChannelHandlerContext ctx) {
		final AttributeKey<BlockManager> SM_KEY = AttributeKey.valueOf("storageManager");
		final BlockManager storageManager = ctx.channel().attr(SM_KEY).get();
		Preconditions.checkNotNull(storageManager);
		return storageManager;
	}
}
