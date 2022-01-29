import {Component, Input, OnInit} from '@angular/core';
import {BackendService} from "../backend.service";
import {ConfigObject} from "./config-object";

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html',
  styleUrls: ['./settings.component.scss']
})
export class SettingsComponent implements OnInit {

  changes = false;
  keyDisplayed = false;
  @Input() apiKey: any;
  services: any;
  regions = {
    Pseudo: {
      platforms: {
        Placeholder: false,
        Placeholder2: false,
      },
      status: false
    }
  };


  constructor(private backend: BackendService) {
    this.services = {};
    this.apiKey = '';
    this.backend.getConfig().subscribe((response: ConfigObject) => {
      this.apiKey = response.apiKey;
      this.regions = response.regions;
      this.services = response.services;
    });
  }

  update_region(region: any): void {
    console.log(region);
    // @ts-ignore
    this.regions[region].status = !this.regions[region].status;
    this.changes = true;
  }

  update_platform(region: any, platform: any): void {
    // @ts-ignore
    this.regions[region].platforms[platform] = !this.regions[region].platforms[platform];
    this.changes = true;
  }

  update_api_key(): void {
    console.log(this.apiKey);
    this.changes = true;

  }

  update_service(service: any): void {
    // @ts-ignore
    this.services[service] = !this.services[service];
    console.log(this.services);
    this.changes = true;
  }

  save_changes(): void {
    console.log('Saving.');
    this.backend.setConfig({regions: this.regions, apiKey: this.apiKey, services: this.services}).subscribe(() => {
      console.log('Updated');
      this.changes = false;
    });
  }

  ngOnInit(): void {
  }

}
