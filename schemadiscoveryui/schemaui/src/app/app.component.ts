import { Component } from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import {CommonModule} from '@angular/common';
import {HttpClientModule} from '@angular/common/http';
import { RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  imports: [CommonModule, HttpClientModule],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
  title = 'schemaui';
  message: string = '';

  constructor(private http: HttpClient) {}

  makeRequest() {
    this.message = 'Button geklickt !!!';
  }


  sendRequest(): void {
    this.http.post('http://127.0.0.1:7659/confirm', {}, {responseType: 'text'})
      .subscribe(response => {
        console.log('Server response:', response);
        alert("Nachricht angekommen.");
        this.message = response;
      }, error => {
        console.error('Error:', error);
        alert("Nachricht nicht angekommen !!!");
      });
  }
}
