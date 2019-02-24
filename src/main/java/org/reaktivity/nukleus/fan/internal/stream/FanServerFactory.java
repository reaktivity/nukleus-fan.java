/**
 * Copyright 2016-2018 The Reaktivity Project
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
    private final LongSupplier supplyCorrelationId;

    private final MessageFunction<RouteFW> wrapRoute;
    private final Long2ObjectHashMap<FanServerGroup> groupsByRouteId;

    public FanServerFactory(
        FanConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        LongSupplier supplyCorrelationId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
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
                    replyTo)::handleStream;
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
            newStream = group::handleStream;
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

        private int budget;
        private int padding;

        private int memberBudget;

        FanServerGroup(
            long routeId)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
            this.members = new CopyOnWriteArrayList<>();

            doBegin(receiver, routeId, initialId, supplyCorrelationId.getAsLong());
            router.setThrottle(initialId, this::handleThrottle);
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(receiver, routeId, initialId);
                break;
            }
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            // ignore (reply stream)
        }

        private void handleData(
            DataFW data)
        {
            final int flags = data.flags();
            final long groupId = data.groupId();
            final int padding = data.padding();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            for (int i=0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doData(member.receiver, member.routeId, member.replyId, flags, groupId, padding, payload, extension);
            }
        }

        private void handleEnd(
            EndFW end)
        {
            for (int i=0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doEnd(member.receiver, member.routeId, member.replyId);
            }

            doEnd(receiver, routeId, replyId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            for (int i=0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doAbort(member.receiver, member.routeId, member.replyId);
            }

            doAbort(receiver, routeId, replyId);
        }

        private void handleReset(
            ResetFW reset)
        {
            for (int i=0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doReset(member.receiver, member.routeId, member.initialId);
            }

            doReset(receiver, routeId, initialId);
        }

        private void handleWindow(
            WindowFW window)
        {
            final int credit = window.credit();
            final int padding = window.padding();
            final long groupId = window.groupId();

            this.budget += credit;
            this.padding = padding;

            for (int i=0; i < members.size(); i++)
            {
                final FanServer member = members.get(i);
                doWindow(member.receiver, member.routeId, member.initialId, credit, padding, groupId);
            }
        }

        @Override
        public String toString()
        {
            return String.format("[%s] routeId=%016x", getClass().getSimpleName(), routeId);
        }

        private void budgetUpdated()
        {
            final int memberCount = members.size();

            if (memberCount != 0)
            {
                int minBudget = Integer.MAX_VALUE;
                int maxPadding = 0;

                for (int i=0; i < memberCount; i++)
                {
                    final FanServer member = members.get(i);
                    minBudget = Math.min(minBudget, member.budget);
                    maxPadding = Math.max(maxPadding, member.padding);
                }

                final int credit = minBudget - memberBudget;
                if (credit > 0)
                {
                    doWindow(receiver, routeId, replyId, credit, maxPadding, 0L);
                    memberBudget = minBudget;
                }
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

        private int budget;
        private int padding;

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

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
                group.join(this);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                group.leave(this);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                group.leave(this);
                break;
            default:
                doReset(receiver, routeId, initialId);
                group.leave(this);
                break;
            }
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                group.leave(this);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long correlationId = begin.correlationId();

            router.setThrottle(replyId, this::handleThrottle);
            doBegin(receiver, routeId, replyId, correlationId);

            if (group.budget > 0)
            {
                doWindow(receiver, routeId, initialId, group.budget, group.padding, 0L);
            }
        }

        private void handleData(
            DataFW data)
        {
            final int flags = data.flags();
            final long groupId = data.groupId();
            final int padding = data.padding();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            // TODO: buffer slot to prevent exceeding budget of fan-in group
            doData(group.receiver, group.routeId, group.initialId, flags, groupId, padding, payload, extension);
        }

        private void handleEnd(
            EndFW end)
        {
            doEnd(receiver, routeId, replyId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            doAbort(receiver, routeId, replyId);
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(receiver, routeId, initialId);
        }

        private void handleWindow(
            WindowFW window)
        {
            final int credit = window.credit();
            final int padding = window.padding();

            this.budget += credit;
            this.padding = padding;

            if (budget > 0) // threshold = 0
            {
                group.budgetUpdated();
            }
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTraceId.getAsLong())
                .correlationId(correlationId)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        int flags,
        long groupId,
        int padding,
        OctetsFW payload,
        OctetsFW extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTraceId.getAsLong())
                .flags(flags)
                .groupId(groupId)
                .padding(padding)
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
                .trace(supplyTraceId.getAsLong())
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
                .trace(supplyTraceId.getAsLong())
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTraceId.getAsLong())
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
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
               .trace(supplyTraceId.getAsLong())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
