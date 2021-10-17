import {Component, Input, OnInit} from '@angular/core';

@Component({
  selector: 'app-platform',
  templateUrl: './platform.component.html',
  styleUrls: ['./platform.component.css']
})
export class PlatformComponent implements OnInit {

  @Input() platform: any;

  constructor() {
  }

  ngOnInit(): void {
    console.log(this.platform);
  }

}
