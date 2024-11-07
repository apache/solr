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

import {Component} from '@angular/core';
import {DateConverter} from "../../../common/utils/DateConverter";
import {SystemInformationService} from "../../services/system-information.service";
import {ISystemInfoResponse} from "../../models/systeminfo/ISystemInfoResponse";
import {SharedModule} from "../../../common/shared.module";


@Component({
  selector: 'app-dashboard',
  templateUrl: './main.component.html',
  styleUrl: './main.component.scss'
})
export class MainComponent {


  private _info!: ISystemInfoResponse;

  constructor(private _systemInformationService: SystemInformationService) {
  }

  ngOnInit() {
    this._systemInformationService.getSystemInfo()
      .then(r => this._info = r);
  }
  get info(): ISystemInfoResponse {
    return this._info;
  }

  averageLoad(): string {
    return this._info.system.systemLoadAverage.toFixed(2);
  }

  isDataAvailable(): boolean {
    return !!this._info
  }

  startTime(): number {
    return this._info.jvm.jmx.upTimeMS
  }

  solrSpecificationVersion(): string {
    return this._info.lucene['solr-spec-version'];
  }

  solrImplementationVersion(): string {
    return this._info.lucene['solr-impl-version'];
  }

  luceneSpecificationVersion(): string {
    return this._info.lucene['lucene-spec-version'];
  }

  luceneImplementationVersion(): string {
    return this._info.lucene['lucene-impl-version'];
  }

  isMemoryInformationAvailable(): boolean {
   return !!(this.maxMemory() && this.freeMemory());
  }

  isSwapInformationAvailable(): boolean {
   return !!(this.swapMax() && this.swapFree());
  }

  maxMemory(): number {
    return this._info.system.totalPhysicalMemorySize;
  }

  availableMemory(): number {
    return this.maxMemory() - this.freeMemory();
  }

  freeJvmMemory(): string {
    return this._info.jvm.memory.free;
  }

  memoryInPercents(): string {
    return this.displayValueInPercents(this.availableMemory(), this.maxMemory());
  }

  private displayValueInPercents(current: number, max: number) {
    return `${(current / max * 100).toFixed(1)} %`;
  }

  swapAvailable(): number {
    return this.swapFree();
  }
  swapMax(): number {
    return this._info.system.totalSwapSpaceSize;
  }

  swapInPercents(): string {
    return this.displayValueInPercents(this.swapAvailable(), this.swapMax());
  }

  fileDescriptors(): string{
    return this.displayValueInPercents(this.openedDescriptors(), this.maxDescriptors());
  }

  openedDescriptors(): number {
    return this._info.system.openFileDescriptorCount;
  }

  maxDescriptors(): number {
    return this._info.system.maxFileDescriptorCount;
  }

  private freeMemory(): number {
    return this._info.system.freePhysicalMemorySize;
  }

  noIformationAvailable(): boolean {
    return !(this.isMemoryInformationAvailable() && this.isSwapInformationAvailable()
      && this.isFileDescriptorInformationAvailable());
  }

  isFileDescriptorInformationAvailable(): boolean {
    return !!(this.openedDescriptors() && this.maxDescriptors());
  }

  private swapFree():number {
    return this._info.system.freeSwapSpaceSize;
  }

  javaMachineInformation(): string {
    return `${this._info.jvm.name} ${this._info.jvm.version}`;
  }

  amountProcessors(): number {
    return this._info.jvm.processors;
  }

  isOldRelease(): boolean {
    let appDateRelease = DateConverter.convertDateFromServerVersion(this.solrImplementationVersion());
   return Date.now() - appDateRelease.getTime() > 365
  }

  upTime(): number {
    return this._info.jvm.jmx.upTimeMS;
  }
  cmdArguments(): string[] {
    return this._info.jvm.jmx.commandLineArgs
  }
  javaMemoryTotal(): number {
    return this._info.jvm.memory.raw.total;
  }
  javaMemoryMax(): number {
    return this._info.jvm.memory.raw.max;
  }
  javaMemoryUsed(): number {
    return this._info.jvm.memory.raw.used;
  }
  javaMemoryUsedPercentage(): string {
    return this._info.jvm.memory.raw["used%"].toFixed(1);
  }

  javaMemoryPercentage(): string {
    return this.displayValueInPercents(this.javaMemoryUsed(), this.javaMemoryMax());
  }

  javaMemoryTotalPercentage(): string {
    return this.displayValueInPercents(this.javaMemoryTotal(), this.javaMemoryMax());
  }

  protected readonly isOdd = SharedModule.isOdd;
}
