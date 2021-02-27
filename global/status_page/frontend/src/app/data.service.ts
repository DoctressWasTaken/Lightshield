import {Injectable} from '@angular/core';
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class DataService {

  status: any;

  constructor(private http: HttpClient) {
    this.status = [];
  }

  update_data(): void {
    this.http.get('https://status.lightshield.dev/api/status')
      .subscribe((data: any) => {
        console.log(data);
        this.status = data;
      });
  }
}
