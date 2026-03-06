import sys

def print_progress_bar(current, total, success, fail, skip, bar_length=40):
    """
    요청하신 스타일의 프로그레스 바 출력 함수
    예시: 📊 [경남] 신규성공=1022, 실패=3, 스킵=0
          [████████████████████████████████████████] 201/201 ✅201
    """
    # 진행률 계산 (0.0 ~ 1.0)
    percent = float(current) / total if total > 0 else 0
    # 채워질 블록(█)과 빈 블록(░) 개수 계산
    filled_length = int(bar_length * percent)
    bar = "█" * filled_length + "░" * (bar_length - filled_length)
    
    # \r 을 사용하여 현재 줄에 덮어쓰기 (캐리지 리턴)
    # verbose=False 일 때도 이 그래프는 보이도록 별도로 처리 가능
    sys.stdout.write(f"\r📊 [DB_MERGE] 신규성공={success}, 실패={fail}, 스킵={skip}\n")
    sys.stdout.write(f"[{bar}] {current}/{total} ✅{success}")
    sys.stdout.flush()
    
    # 100% 도달 시 줄바꿈 처리
    if current == total:
        print() 

# ... (merge_databases 함수 내부 수정 예시) ...

    total_rows = 0
    for shard_db in shard_dbs:
        shard_name = os.path.basename(shard_db).replace("neis_info_", "").replace(".db", "")
        if verbose:
            print(f"\n📦 병합 시작: {shard_name}")
            
        shard_conn = sqlite3.connect(f"file:{shard_db}?mode=ro", uri=True)
        shard_conn.row_factory = sqlite3.Row
        cur = shard_conn.execute("SELECT * FROM schools")
        rows = cur.fetchall()
        
        total_in_shard = len(rows)
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        # 배치 처리를 위한 변수
        batch_size = 100
        insert_data = []
        
        for i, row in enumerate(rows):
            insert_data.append((
                row['sc_code'], row['school_id'], row['sc_name'], row['eng_name'],
                row['sc_kind'], row['atpt_code'], row['address'], row['address_hash'],
                row['tel'], row['homepage'], row['status'], row['last_seen'], row['load_dt'],
                row['latitude'], row['longitude'], row['city_id'], row['district_id'],
                row['street_id'], row['number_bit']
            ))
            
            # 배치 단위 삽입 및 진행도 업데이트
            if len(insert_data) >= batch_size or i == total_in_shard - 1:
                try:
                    conn.executemany("""
                        INSERT OR REPLACE INTO schools VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, insert_data)
                    conn.commit()
                    success_count += len(insert_data)
                except Exception as e:
                    fail_count += len(insert_data)
                    if verbose: print(f"❌ 오류: {e}")
                insert_data = [] # 배치 초기화
            
            # 실시간 그래프 출력 (100 건 단위 또는 마지막)
            # 너무 잦은 출력은 터미널을 느리게 하므로 조절이 필요할 수 있습니다.
            if (i + 1) % 100 == 0 or (i + 1) == total_in_shard:
                # 전체 누적 통계 업데이트
                current_total = total_rows + success_count + fail_count + skip_count
                print_progress_bar(
                    current=current_total, 
                    total=total_rows + total_in_shard, # 전체 예상 작업량
                    success=total_rows + success_count,
                    fail=fail_count,
                    skip=skip_count
                )

        total_rows += success_count + fail_count + skip_count
        shard_conn.close()

    # 마지막 정리 출력
    print(f"\n✅ 병합 완료: {total_rows:,}건")
    