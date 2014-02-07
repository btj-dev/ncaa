INSERT INTO play ( game_code, play_number, quarter, clock, offense, defense,
       offense_points, defense_points, down, distance, spot, play_type, 
       drive_number, drive_play ) VALUES ( :game_code, :play_number, :quarter,
       :clock, :offense, :defense, :offense_points, :defense_points, :down,
       :distance, :spot, ':play_type', :drive_number, :drive_play );
