import {Component, Input, OnInit} from '@angular/core';

@Component({
  selector: 'app-overview',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.scss']
})
export class OverviewComponent implements OnInit {

  selected_queues: any;
  selected_servers: any;
  queues: any[] = [420, 430, 440, 450];
  servers: any[] = ['BR1', 'EUN1', 'EUW1', 'JP1', 'KR', 'LA1', 'LA2', 'NA1', 'OC1', 'TR1', 'RU'];
  non_ranked: boolean;
  patch_backtracking: number;

  constructor() {
    this.non_ranked = false;
    this.patch_backtracking = 2;
  }

  ngOnInit(): void {
  }

  clicked() {
    console.log(this.selected_queues, this.selected_servers, this.non_ranked, this.patch_backtracking);
  }

  update() {
    console.log("Here")
  }

}
