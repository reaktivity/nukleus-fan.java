/**
 * Copyright 2016-2021 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.fan.internal.stream;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.fan.internal.FanConfiguration;
import org.reaktivity.nukleus.fan.internal.types.OctetsFW;
import org.reaktivity.nukleus.fan.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.fan.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.fan.internal.types.stream.DataFW;
import org.reaktivity.nukleus.fan.internal.types.stream.EndFW;
import org.reaktivity.nukleus.fan.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.fan.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.fan.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;

public final class FanServerFactory implements FanStreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final Long2ObjectHashMap<Binding> bindings;

    private final MutableDirectBuffer writeBuffer;
    private final StreamFactory streamFactory;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<FanServerGroup> groupsByRouteId;

    public FanServerFactory(
        FanConfiguration config,
        ElektronContext context)
    {
        this.writeBuffer = context.writeBuffer();
        this.streamFactory = context.streamFactory();
        this.supplyInitialId = context::supplyInitialId;
        this.supplyReplyId = context::supplyReplyId;
        this.supplyTraceId = context::supplyTraceId;
        this.bindings = new Long2ObjectHashMap<>();
        this.groupsByRouteId = new Long2ObjectHashMap<>();
    }

    @Override
    public void attach(
        Binding binding)
    {
        bindings.put(binding.id, binding);
    }

    @Override
    public void detach(
        long bindingId)
    {
        bindings.remove(bindingId);
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer replyTo)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final Binding binding = bindings.get(routeId);

        MessageConsumer newStream = null;

        if (binding != null)
        {
            final long initialId = begin.streamId();
            final long replyId = supplyReplyId.applyAsLong(initialId);

            final FanServerGroup group = supplyFanServerGroup(binding.exit.id);

            newStream = new FanServer(
                    group,
                    routeId,
                    initialId,
                    replyId,
                    replyTo)::onMemberMessage;
        }

        return newStream;
    }

    private FanServerGroup supplyFanServerGroup(
        long routeId)
    {
        return groupsByRouteId.computeIfAbsent(routeId, FanServerGroup::new);
    }

    private final class FanServerGroup
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;
        private final List<FanServer> members;

        private long initialSeq;
        private long initialAck;
        private int initialMax;
        private int initialPad;
        private long initialBudgetId;

        private long replySeq;
        private long replyAck;
        private int replyMax;

        private boolean replyInitiated;

        FanServerGroup(
            long routeId)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.members = new CopyOnWriteArrayList<>();

            long traceId = supplyTraceId.getAsLong();
            this.receiver = newStream(this::onGroupMessage, routeId,
                    initialId, initialSeq, initialAck, initialMax, traceId, 0L, 0L, e -> {});
        }

        private void onGroupMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onGroupBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onGroupData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onGroupEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onGroupAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onGroupFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onGroupReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onGroupWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onGroupBegin(
            BeginFW begin)
        {
            this.replyInitiated = true;

            final long traceId = begin.traceId();
            final long affinity = begin.affinity();
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                member.doMemberBegin(traceId, affinity);
            }
        }

        private void onGroupData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            assert acknowledge <= sequence;
            assert sequence >= replySeq;

            replySeq = sequence + reserved;

            assert replyAck <= replySeq;

            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                member.doMemberData(traceId, flags, budgetId, reserved, payload, extension);
            }
        }

        private void onGroupEnd(
            EndFW end)
        {
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doEnd(member.receiver, member.routeId, member.replyId,
                        member.replySeq, member.replyAck, member.replyMax);
            }

            doEnd(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onGroupAbort(
            AbortFW abort)
        {
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doAbort(member.receiver, member.routeId, member.replyId,
                        member.replySeq, member.replyAck, member.replyMax);
            }

            doAbort(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onGroupFlush(
            FlushFW flush)
        {
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();

            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doFlush(member.receiver, member.routeId, member.replyId,
                        member.replySeq, member.replyAck, member.replyMax,
                        budgetId, reserved);
            }
        }

        private void onGroupReset(
            ResetFW reset)
        {
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doReset(member.receiver, member.routeId, member.initialId,
                        member.initialSeq, member.initialAck, member.initialMax);
            }

            doReset(receiver, routeId, initialId, initialSeq, initialAck, initialMax);
        }

        private void onGroupWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= initialSeq;
            assert acknowledge >= initialAck;
            assert maximum >= initialMax;

            this.initialAck = acknowledge;
            this.initialMax = maximum;
            this.initialPad = padding;
            this.initialBudgetId = budgetId;

            assert initialAck <= initialSeq;

            final int pendingAck = (int)(initialSeq - initialAck);
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                member.doMemberWindow(traceId, budgetId, initialMax, pendingAck, initialPad);
            }
        }

        @Override
        public String toString()
        {
            return String.format("[%s] routeId=%016x", getClass().getSimpleName(), routeId);
        }

        private void doGroupData(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            doData(receiver, routeId, initialId, initialSeq, initialAck, initialMax,
                    traceId, flags, budgetId, reserved, payload, extension);

            initialSeq += reserved;
            assert initialSeq <= initialAck + initialMax;
        }

        private void doGroupFlush(
            long traceId,
            long budgetId,
            int reserved)
        {
            doFlush(receiver, routeId, initialId, initialSeq, initialAck, initialMax, budgetId, reserved);
        }

        private void doGroupWindow(
            long traceId,
            long budgetId,
            int windowMax,
            int pendingAck,
            int paddingMin)
        {
            long replyAckMax = Math.max(replySeq - pendingAck, replyAck);
            if (replyAckMax > replyAck || windowMax > replyMax)
            {
                replyAck = replyAckMax;
                replyMax = windowMax;
                assert replyAck <= replySeq;

                doWindow(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, budgetId, paddingMin);
            }
        }

        private void join(
            FanServer member)
        {
            members.add(member);
        }

        private void leave(
            FanServer member)
        {
            members.remove(member);
        }
    }

    private final class FanServer
    {
        private final FanServerGroup group;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private long initialSeq;
        private long initialAck;
        private int initialMax;

        private long replySeq;
        private long replyAck;
        private int replyMax;
        private int replyPad;
        private boolean replyInitiated;

        private FanServer(
            FanServerGroup group,
            long routeId,
            long initialId,
            long replyId,
            MessageConsumer receiver)
        {
            this.group = group;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.receiver = receiver;
        }

        private void onMemberMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onMemberBegin(begin);
                group.join(this);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onMemberData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onMemberEnd(end);
                group.leave(this);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onMemberAbort(abort);
                group.leave(this);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onMemberFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onMemberReset(reset);
                group.leave(this);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onMemberWindow(window);
                break;
            default:
                break;
            }
        }

        private void onMemberBegin(
            BeginFW begin)
        {
            final long affinity = begin.affinity();

            doMemberBegin(supplyTraceId.getAsLong(), affinity);
            doMemberWindow(supplyTraceId.getAsLong(), group.initialBudgetId, group.initialMax,
                    (int)(group.initialSeq - group.initialAck), group.initialPad);
        }

        private void onMemberData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence + data.reserved();

            assert initialAck <= initialSeq;

            // TODO: buffer slot to prevent exceeding budget of fan-in group
            group.doGroupData(traceId, flags, budgetId, reserved, payload, extension);
        }

        private void onMemberEnd(
            EndFW end)
        {
            doEnd(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onMemberAbort(
            AbortFW abort)
        {
            doAbort(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onMemberFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();

            group.doGroupFlush(traceId, budgetId, reserved);
        }

        private void onMemberReset(
            ResetFW reset)
        {
            doReset(receiver, routeId, initialId, initialSeq, initialAck, initialMax);
        }

        private void onMemberWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= replySeq;
            assert acknowledge >= replyAck;
            assert maximum >= replyMax;

            this.replyAck = acknowledge;
            this.replyMax = maximum;
            this.replyPad = padding;

            assert replyAck <= replySeq;

            group.doGroupWindow(traceId, budgetId, replyMax, (int)(replySeq - replyAck), replyPad);
        }

        private void doMemberWindow(
            long traceId,
            long budgetId,
            int windowMax,
            int pendingAck,
            int paddingMin)
        {
            long initialAckMax = Math.max(initialSeq - pendingAck, initialAck);
            if (initialAckMax > initialAck || windowMax > initialMax)
            {
                initialAck = initialAckMax;
                initialMax = windowMax;
                assert initialAck <= initialSeq;

                doWindow(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, budgetId, paddingMin);
            }
        }

        private void doMemberBegin(
            long traceId,
            long affinity)
        {
            if (group.replyInitiated && !replyInitiated)
            {
                doBegin(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, affinity);
                replyInitiated = true;
            }
        }

        private void doMemberData(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            doData(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, flags,
                    budgetId, reserved, payload, extension);

            replySeq += reserved;
            assert replySeq <= replyAck + replyMax;
        }
    }

    private MessageConsumer newStream(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        Consumer<OctetsFW.Builder> extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension)
                .build();

        MessageConsumer receiver =
                streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), sender);

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        return receiver;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long affinity)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .affinity(affinity)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        int flags,
        long budgetId,
        int reserved,
        OctetsFW payload,
        OctetsFW extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .extension(extension)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(supplyTraceId.getAsLong())
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long budgetId,
        int reserved)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(supplyTraceId.getAsLong())
                .budgetId(budgetId)
                .reserved(reserved)
                .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(supplyTraceId.getAsLong())
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long budgetId,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .budgetId(budgetId)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(supplyTraceId.getAsLong())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
