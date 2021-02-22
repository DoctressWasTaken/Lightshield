import {Component} from '@angular/core';
import {DataService} from "./data.service";

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  title = 'status-page';

  data: DataService;

  constructor(private d: DataService) {
    this.data = d;
  }

  ngOnInit(): void {
    this.data.update_data.bind(this.data);
    setInterval(this.data.update_data.bind(this.data), 60000);
  }
}
