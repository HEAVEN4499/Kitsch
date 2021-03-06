package kitsch.rdd
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.typesafe.scalalogging.slf4j.LazyLogging
import kitsch.Kitsch

private[kitsch] object RDDOperationScope extends LazyLogging {
  private[kitsch] def withScope[T](kitsch: Kitsch,
                                   allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        logger.warn("No valid method name for this RDD operation scope!")
        "N/A"
      }
    println(s"[Kitsch-INFO] Caller methed name: $body.$callerMethodName")
    try {
      body
    } catch {
      case e: Throwable =>
        logger.error(callerMethodName, e)
        throw e
    }
  }
}
