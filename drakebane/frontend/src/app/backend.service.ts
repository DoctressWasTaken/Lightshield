import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {ConfigObject} from './settings/config-object';

const url = 'http://localhost:8302';

@Injectable({
  providedIn: 'root'
})
export class BackendService {

  constructor(private http: HttpClient) {
  }

  getConfig(): any {
    return this.http.get<ConfigObject>(url + '/config');
  }

  setConfig(config: any): any {
    return this.http.post<any>(
      url + '/config', JSON.stringify(config));
  }

}
