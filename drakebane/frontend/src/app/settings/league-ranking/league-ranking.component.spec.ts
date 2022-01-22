import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LeagueRankingComponent } from './league-ranking.component';

describe('LeagueRankingComponent', () => {
  let component: LeagueRankingComponent;
  let fixture: ComponentFixture<LeagueRankingComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LeagueRankingComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LeagueRankingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
