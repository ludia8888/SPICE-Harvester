#!/usr/bin/env python3
"""
OMS 라우터 경로 순서 재정렬
더 구체적인 경로를 먼저 정의하도록 수정
"""

import re

# OMS ontology router 파일 읽기
with open('/Users/isihyeon/Desktop/SPICE HARVESTER/backend/ontology-management-service/routers/ontology.py', 'r') as f:
    content = f.read()

# analyze-network 엔드포인트 찾기
analyze_network_pattern = r'(@router\.get\("/analyze-network"\).*?)(?=@router\.|$)'
analyze_network_match = re.search(analyze_network_pattern, content, re.DOTALL)

if analyze_network_match:
    analyze_network_code = analyze_network_match.group(1)
    print("Found analyze-network endpoint")
    
    # 원본에서 제거
    content_without_analyze = content.replace(analyze_network_code, '')
    
    # /{class_id} 엔드포인트 찾기
    class_id_pattern = r'(@router\.get\("/{class_id}".*?)(?=@router\.|$)'
    class_id_match = re.search(class_id_pattern, content_without_analyze, re.DOTALL)
    
    if class_id_match:
        # /{class_id} 앞에 analyze-network 삽입
        insert_pos = class_id_match.start()
        new_content = (
            content_without_analyze[:insert_pos] + 
            analyze_network_code + 
            "\n\n" +
            content_without_analyze[insert_pos:]
        )
        
        # 파일에 쓰기
        with open('/Users/isihyeon/Desktop/SPICE HARVESTER/backend/ontology-management-service/routers/ontology.py', 'w') as f:
            f.write(new_content)
        
        print("Successfully reordered routes!")
    else:
        print("Could not find /{class_id} endpoint")
else:
    print("Could not find analyze-network endpoint")