/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvstore

import java.util.UUID

import se.kth.id2203.networking.NetAddress
import se.sics.kompics.KompicsEvent;

trait Operation extends KompicsEvent {
  def id: UUID;
  def key: String;
  def source: NetAddress;
}

@SerialVersionUID(0xfacc6612da2139eaL)
abstract class Op extends Operation {
  def response(status: OpCode.OpCode, value: Option[String]): OpResponse = OpResponse(id, status, value);
}

@SerialVersionUID(0xfacc6612da2139eaL)
case class Get(key: String, source: NetAddress, id: UUID = UUID.randomUUID()) extends Op;

@SerialVersionUID(0xfacc6612da2139eaL)
case class Put(key: String, value: String, source: NetAddress, id: UUID = UUID.randomUUID()) extends Op

@SerialVersionUID(0xfacc6612da2139eaL)
case class Cas(key: String, oldValue:String, newValue:String, source: NetAddress, id: UUID = UUID.randomUUID()) extends Op

@SerialVersionUID(0xfacc6612da2136eaL)
case class Debug(key: String, source: NetAddress, id: UUID = UUID.randomUUID()) extends Op

object OpCode {
  sealed trait OpCode;
  case object Ok extends OpCode;
  case object NonMatchingValues extends OpCode;
  case object NotFound extends OpCode;
  case object NotImplemented extends OpCode;
}

trait OperationResponse extends KompicsEvent {
  def id: UUID;
  def status: OpCode.OpCode;
  def value: Option[String]
}

@SerialVersionUID(0x0227a2aea45e5e75L)
case class OpResponse(id: UUID, status: OpCode.OpCode, value: Option[String]) extends OperationResponse with Serializable;
