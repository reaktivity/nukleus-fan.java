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

        private int initialBudget;
        private int initialPadding;

        private int replyBudget;
        private int replyPadding;

        private boolean replyInitiated;

        FanServerGroup(
            long routeId)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
            this.members = new CopyOnWriteArrayList<>();

            doBegin(receiver, routeId, initialId, supplyTraceId.getAsLong(), 0L);
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
                doReset(receiver, routeId, initialId);
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
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            replyBudget -= reserved;

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
                doEnd(member.receiver, member.routeId, member.replyId);
            }

            doEnd(receiver, routeId, replyId);
        }

        private void onAbort(
            AbortFW abort)
        {
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doAbort(member.receiver, member.routeId, member.replyId);
            }

            doAbort(receiver, routeId, replyId);
        }

        private void onReset(
            ResetFW reset)
        {
            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doReset(member.receiver, member.routeId, member.initialId);
            }

            doReset(receiver, routeId, initialId);
        }

        private void onWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            this.initialBudget += credit;
            this.initialPadding = padding;

            for (int i = 0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                member.sendInitialWindow(traceId, budgetId, initialBudget, padding);
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
            initialBudget -= reserved;
            doData(receiver, routeId, initialId, traceId, flags, budgetId, reserved, payload, extension);
        }

        private void sendReplyWindow(
            int minReplyBudget,
            int minReplyPadding,
            long traceId)
        {
            final int newReplyBudget = Math.max(replyBudget, minReplyBudget);
            final int newReplyPadding = Math.max(replyPadding, minReplyPadding);

            replyPadding = newReplyPadding;

            final int replyCredit = newReplyBudget - replyBudget;
            if (replyCredit > 0)
            {
                doWindow(receiver, routeId, replyId, traceId, 0L, replyCredit, newReplyPadding);
                replyBudget = newReplyBudget;
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

        private int initialBudget;
        private int replyBudget;
        private int replyPadding;
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
            sendInitialWindow(supplyTraceId.getAsLong(), 0L, group.initialBudget, group.initialPadding);
        }

        private void onData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            // TODO: buffer slot to prevent exceeding budget of fan-in group
            initialBudget -= reserved;
            group.sendInitialData(traceId, flags, budgetId, reserved, payload, extension);
        }

        private void onEnd(
            EndFW end)
        {
            doEnd(receiver, routeId, replyId);
        }

        private void onAbort(
            AbortFW abort)
        {
            doAbort(receiver, routeId, replyId);
        }

        private void onReset(
            ResetFW reset)
        {
            doReset(receiver, routeId, initialId);
        }

        private void onWindow(
            WindowFW window)
        {
            final int credit = window.credit();
            final int padding = window.padding();

            this.replyBudget += credit;
            this.replyPadding = padding;

            if (credit > 0 && replyBudget > 0) // threshold = 0
            {
                final long traceId = window.traceId();
                group.sendReplyWindow(replyBudget, replyPadding, traceId);
            }
        }

        private void sendInitialWindow(
            long traceId,
            long budgetId,
            int maxInitialBudget,
            int minInitialPadding)
        {
            final int initialCredit = maxInitialBudget - initialBudget;
            if (initialCredit > 0)
            {
                doWindow(receiver, routeId, initialId, traceId, budgetId, initialCredit, minInitialPadding);
                initialBudget = maxInitialBudget;
            }
        }

        private void sendReplyBegin(
            long traceId,
            long affinity)
        {
            if (group.replyInitiated && !replyInitiated)
            {
                router.setThrottle(replyId, this::onMessage);
                doBegin(receiver, routeId, replyId, traceId, affinity);
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
            replyBudget -= reserved;
            doData(receiver, routeId, replyId, traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long affinity)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .affinity(affinity)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
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
        long streamId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(supplyTraceId.getAsLong())
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(supplyTraceId.getAsLong())
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final long budgetId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(supplyTraceId.getAsLong())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
