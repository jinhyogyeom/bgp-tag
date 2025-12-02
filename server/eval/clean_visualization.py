import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 폰트 설정 - 한글 문제 해결
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

def load_and_analyze_data():
    """데이터 로드 및 분석"""
    # 데이터 로드
    flap_data = []
    hijack_data = []
    loop_data = []
    
    # FLAP 데이터
    with open('/app/eval/flap/flap_graded_results.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                flap_data.append(json.loads(line))
    
    # HIJACK 데이터
    with open('/app/eval/hijack/hijack_graded_results.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                hijack_data.append(json.loads(line))
    
    # LOOP 데이터
    with open('/app/eval/loop/loop_graded_results.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                loop_data.append(json.loads(line))
    
    return flap_data, hijack_data, loop_data

def create_clean_summary():
    """깔끔한 요약 차트 하나만 생성"""
    flap_data, hijack_data, loop_data = load_and_analyze_data()
    
    # 점수 데이터 추출
    def extract_scores(data, test_name):
        scores = []
        for item in data:
            if 'score' in item and item['score']:
                score = item['score']
                score['test_type'] = test_name
                score['success'] = item.get('success', False)
                scores.append(score)
        return scores
    
    flap_scores = extract_scores(flap_data, 'FLAP')
    hijack_scores = extract_scores(hijack_data, 'HIJACK')
    loop_scores = extract_scores(loop_data, 'LOOP')
    
    # 전체 데이터 합치기
    all_scores = flap_scores + hijack_scores + loop_scores
    
    # DataFrame 생성
    df = pd.DataFrame(all_scores)
    
    # 통계 계산
    summary_stats = []
    for test_type in ['FLAP', 'HIJACK', 'LOOP']:
        test_df = df[df['test_type'] == test_type]
        stats = {
            'Test Type': test_type,
            'Total Tests': len(test_df),
            'Success Rate (%)': round(test_df['success'].mean() * 100, 1),
            'Avg Total Score': round(test_df['총점'].mean(), 2),
            'Avg Execution': round(test_df['실행여부'].mean(), 2),
            'Avg Event Type': round(test_df['이벤트종류'].mean(), 2),
            'Avg Time Range': round(test_df['시간범위'].mean(), 2),
            'Avg Numeric Match': round(test_df['수치일치'].mean(), 2),
            'Avg Explanation': round(test_df['설명품질'].mean(), 2),
            'Max Score': test_df['총점'].max(),
            'Min Score': test_df['총점'].min()
        }
        summary_stats.append(stats)
    
    # 시각화 - 하나의 깔끔한 차트
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('BGP Anomaly Detection Test Results', fontsize=16, fontweight='bold')
    
    # 1. 성공률 비교
    test_types = ['FLAP', 'HIJACK', 'LOOP']
    success_rates = [df[df['test_type'] == t]['success'].mean() * 100 for t in test_types]
    
    bars1 = ax1.bar(test_types, success_rates, color=['#FF6B6B', '#4ECDC4', '#45B7D1'], alpha=0.8)
    ax1.set_title('Success Rate by Test Type', fontweight='bold')
    ax1.set_ylabel('Success Rate (%)')
    ax1.set_ylim(0, 110)
    
    # 값 표시
    for bar, rate in zip(bars1, success_rates):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2, 
                f'{rate:.1f}%', ha='center', fontweight='bold')
    
    # 2. 평균 총점 비교
    avg_scores = [df[df['test_type'] == t]['총점'].mean() for t in test_types]
    
    bars2 = ax2.bar(test_types, avg_scores, color=['#FF6B6B', '#4ECDC4', '#45B7D1'], alpha=0.8)
    ax2.set_title('Average Total Score by Test Type', fontweight='bold')
    ax2.set_ylabel('Average Score')
    ax2.set_ylim(0, 10)
    
    # 값 표시
    for bar, score in zip(bars2, avg_scores):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'{score:.2f}', ha='center', fontweight='bold')
    
    # 3. 카테고리별 성능 비교
    categories = ['Execution', 'Event Type', 'Time Range', 'Numeric Match', 'Explanation']
    category_keys = ['실행여부', '이벤트종류', '시간범위', '수치일치', '설명품질']
    
    x = np.arange(len(categories))
    width = 0.25
    
    for i, test_type in enumerate(test_types):
        test_df = df[df['test_type'] == test_type]
        category_means = [test_df[key].mean() for key in category_keys]
        ax3.bar(x + i*width, category_means, width, label=test_type, alpha=0.8)
    
    ax3.set_title('Category Performance Comparison', fontweight='bold')
    ax3.set_xlabel('Categories')
    ax3.set_ylabel('Average Score')
    ax3.set_xticks(x + width)
    ax3.set_xticklabels(categories, rotation=45, ha='right')
    ax3.legend()
    ax3.set_ylim(0, 3.5)
    
    # 4. 점수 분포 (박스플롯)
    box_data = [df[df['test_type'] == t]['총점'] for t in test_types]
    bp = ax4.boxplot(box_data, labels=test_types, patch_artist=True)
    
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax4.set_title('Score Distribution', fontweight='bold')
    ax4.set_ylabel('Total Score')
    
    plt.tight_layout()
    plt.savefig('/app/eval/clean_test_results.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # 요약 테이블 출력
    summary_df = pd.DataFrame(summary_stats)
    print("\n=== BGP Anomaly Detection Test Results Summary ===")
    print(summary_df.to_string(index=False))
    
    return summary_df

if __name__ == "__main__":
    print("Creating clean BGP test results visualization...")
    summary = create_clean_summary()
    print(f"\nVisualization saved as: clean_test_results.png")
