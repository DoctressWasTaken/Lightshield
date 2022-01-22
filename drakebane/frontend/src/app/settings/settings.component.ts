import {Component, Input, OnInit} from '@angular/core';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.component.html',
  styleUrls: ['./settings.component.scss']
})
export class SettingsComponent implements OnInit {

  changes = false;
  keyDisplayed = false;
  @Input() apiKey: string;
  regions = {
    Europe: {
      platforms: {EUW: true, EUNE: false, TR: false, RU: false},
      status: true,
    },
    Americas: {
      platforms: {NA: true, BR: false, LAN: false, LAS: false, OCE: false},
      status: true,
    },
    Asia: {
      platforms: {KR: true, JP: false},
      status: true,
    }
  };

  constructor() {
    this.apiKey = '';
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
  update_api_key(): void{
    console.log(this.apiKey);
  }
  save_changes(): void {
  }

  ngOnInit(): void {
  }

}
