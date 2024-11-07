/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import {Pipe, PipeTransform} from '@angular/core';

@Pipe({
  name: 'timeAgo'
})
export class TimeAgoPipe implements PipeTransform {
  private static YEAR_MS: number = 1000 * 60 * 60 * 24 * 7 * 4 * 12;
  private static MAPPER: {single:string, many:string, div: number}[] = [
    { single: 'a year ago', many: 'years', div: 1 },
    { single: 'a month ago', many: 'months', div: 12 },
    { single: 'a week ago', many: 'weeks', div: 4 },
    { single: 'a day ago', many: 'days', div: 7 },
    { single: 'an hour ago', many: 'hours', div: 24 },
    { single: 'a minute ago', many: 'minutes', div: 60 },
    ]
  transform(value: number): string {
    if (!value) {
      return 'Invalid start time';
    }

    for (let i: number = 0, l = TimeAgoPipe.MAPPER.length,  div: number = TimeAgoPipe.YEAR_MS; i < l; ++i) {
      let elm = TimeAgoPipe.MAPPER[i];
      let unit: number = Math.floor(value / (div /= elm.div));

      if (unit >= 1) {
        return unit === 1 ? elm.single : `${unit} ${elm.many} ago`;
      }
    }

    return 'less than a minute';
  }

}
