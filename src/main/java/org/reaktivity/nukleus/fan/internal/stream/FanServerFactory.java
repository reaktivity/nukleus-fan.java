/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.fan.internal.FanConfiguration;
import org.reaktivity.nukleus.fan.internal.types.OctetsFW;
import org.reaktivity.nukleus.fan.internal.types.control.RouteFW;
import org.reaktivity.nukleus.fan.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.fan.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.fan.internal.types.stream.DataFW;
import org.reaktivity.nukleus.fan.internal.types.stream.EndFW;
import org.reaktivity.nukleus.fan.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.fan.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class FanServerFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final MessageFunction<RouteFW> wrapRoute;
    private final Long2ObjectHashMap<FanServerGroup> groupsByRouteId;

    public FanServerFactory(
        FanConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.wrapRoute = this::wrapRoute;
        this.groupsByRouteId = new Long2ObjectHashMap<>();
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
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, replyTo);
        }
        else
        {
            newStream = newReplyStream(begin, replyTo);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer replyTo)
    {
        final long routeId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long initialId = begin.streamId();
            final long replyId = supplyReplyId.applyAsLong(initialId);

            final FanServerGroup group = supplyFanServerGroup(route.correlationId());

            newStream = new FanServer(
                    group,
                    routeId,
                    initialId,
                    replyId,
                    replyTo)::onMessage;
        }

        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer replyTo)
    {
        final long routeId = begin.routeId();
        final long replyId = begin.streamId();
        final FanServerGroup group = groupsByRouteId.get(routeId);

        MessageConsumer newStream = null;

        if (group != null && group.replyId == replyId)
        {
            assert group.receiver == replyTo;
            newStream = group::onStream;
        }

        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
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
            this.receiver = router.supplyReceiver(initialId);
            this.members = new CopyOnWriteArrayList<>();

            doBegin(receiver, routeId, initialId, initialSeq, initialAck, initialMax, supplyTraceId.getAsLong(), 0L);
            router.setThrottle(initialId, this::onThrottle);
        }

        private void onStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            default:
                doReset(receiver, routeId, initialId, initialSeq, initialAck, initialMax);
                break;
            }
        }

        private void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            this.replyInitiated = true;

            final long traceId = begin.traceId();
            final long affinity = begin.affinity();
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                member.sendReplyBegin(traceId, affinity);
            }
        }

        private void onData(
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
                member.sendReplyData(traceId, flags, budgetId, reserved, payload, extension);
            }
        }

        private void onEnd(
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

        private void onAbort(
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

        private void onReset(
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

        private void onWindow(
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
                member.sendInitialWindow(traceId, budgetId, initialMax, pendingAck, initialPad);
            }
        }

        @Override
        public String toString()
        {
            return String.format("[%s] routeId=%016x", getClass().getSimpleName(), routeId);
        }

        private void sendInitialData(
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

        private void sendReplyWindow(
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

        private void onMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                group.join(this);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                group.leave(this);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                group.leave(this);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                group.leave(this);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            default:
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long affinity = begin.affinity();

            sendReplyBegin(supplyTraceId.getAsLong(), affinity);
            sendInitialWindow(supplyTraceId.getAsLong(), group.initialBudgetId, group.initialMax,
                    (int)(group.initialSeq - group.initialAck), group.initialPad);
        }

        private void onData(
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
            group.sendInitialData(traceId, flags, budgetId, reserved, payload, extension);
        }

        private void onEnd(
            EndFW end)
        {
            doEnd(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onAbort(
            AbortFW abort)
        {
            doAbort(receiver, routeId, replyId, replySeq, replyAck, replyMax);
        }

        private void onReset(
            ResetFW reset)
        {
            doReset(receiver, routeId, initialId, initialSeq, initialAck, initialMax);
        }

        private void onWindow(
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

            group.sendReplyWindow(traceId, budgetId, replyMax, (int)(replySeq - replyAck), replyPad);
        }

        private void sendInitialWindow(
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

        private void sendReplyBegin(
            long traceId,
            long affinity)
        {
            if (group.replyInitiated && !replyInitiated)
            {
                router.setThrottle(replyId, this::onMessage);
                doBegin(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, affinity);
                replyInitiated = true;
            }
        }

        private void sendReplyData(
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
