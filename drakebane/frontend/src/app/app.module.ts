import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent } from './app.component';
import { OverviewComponent } from './overview/overview.component';
import { RegionComponent } from './overview/region/region.component';
import { PlatformComponent } from './overview/platform/platform.component';
import { GlobalComponent } from './overview/global/global.component';
import {FormsModule} from "@angular/forms";

@NgModule({
  declarations: [
    AppComponent,
    OverviewComponent,
    RegionComponent,
    PlatformComponent,
    GlobalComponent,
  ],
  imports: [
    FormsModule,
    BrowserModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
