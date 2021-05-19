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
package org.reaktivity.nukleus.fan.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

public class ServerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/fan/streams/network")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/fan/streams/application");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .configurationRoot("org/reaktivity/specification/nukleus/fan/config")
        .external("app#0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.connected/client",
        "${app}/client.connected/server"})
    public void shouldConnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.sent.data/client",
        "${app}/client.sent.data/server"})
    public void shouldReceiveClientSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.received.data/client",
        "${app}/client.received.data/server"})
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.sent.and.received.data/client",
        "${app}/client.sent.and.received.data/server"})
    public void shouldEchoClientSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.received.data/client",
        "${net}/client.received.data/client",
        "${net}/client.received.data/client",
        "${net}/client.received.data/client",
        "${net}/client.sent.and.received.data/client",
        "${app}/client.sent.and.received.data/server"})
    public void shouldEchoAndFanoutClientData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.sent.flush/client",
        "${app}/client.sent.flush/server"})
    public void shouldReceiveClientSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/server.sent.flush/client",
        "${app}/server.sent.flush/server"})
    public void shouldReceiveServerSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/server.sent.flush/client",
        "${net}/server.sent.flush/client",
        "${net}/server.sent.flush/client",
        "${net}/server.sent.flush/client",
        "${net}/server.sent.flush/client",
        "${app}/server.sent.flush/server"})
    public void shouldFanoutServerSentFlush() throws Exception
    {
        k3po.finish();
    }
}
