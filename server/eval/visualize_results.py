import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib import font_manager
import warnings
warnings.filterwarnings('ignore')

# 한글 폰트 설정
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

def load_jsonl_data(file_path):
    """JSONL 파일을 로드하여 DataFrame으로 변환"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return pd.DataFrame(data)

def extract_score_data(df):
    """점수 데이터 추출 및 정리"""
    score_data = []
    for idx, row in df.iterrows():
        if 'score' in row and row['score']:
            score_dict = row['score']
            score_dict['test_id'] = idx
            score_dict['success'] = row.get('success', False)
            score_dict['input'] = row.get('input', '')
            score_data.append(score_dict)
    return pd.DataFrame(score_data)

def create_overview_charts():
    """전체 개요 차트 생성"""
    # 데이터 로드
    flap_df = load_jsonl_data('/app/eval/flap/flap_graded_results.jsonl')
    hijack_df = load_jsonl_data('/app/eval/hijack/hijack_graded_results.jsonl')
    loop_df = load_jsonl_data('/app/eval/loop/loop_graded_results.jsonl')
    
    # 점수 데이터 추출
    flap_scores = extract_score_data(flap_df)
    hijack_scores = extract_score_data(hijack_df)
    loop_scores = extract_score_data(loop_df)
    
    # 테스트 타입 추가
    flap_scores['test_type'] = 'FLAP'
    hijack_scores['test_type'] = 'HIJACK'
    loop_scores['test_type'] = 'LOOP'
    
    # 전체 데이터 합치기
    all_scores = pd.concat([flap_scores, hijack_scores, loop_scores], ignore_index=True)
    
    # 1. 전체 성공률 비교
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('BGP Anomaly Detection Test Results Overview', fontsize=16, fontweight='bold')
    
    # 성공률 비교
    success_rate = all_scores.groupby('test_type')['success'].mean()
    axes[0, 0].bar(success_rate.index, success_rate.values, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
    axes[0, 0].set_title('Success Rate by Test Type')
    axes[0, 0].set_ylabel('Success Rate')
    axes[0, 0].set_ylim(0, 1)
    for i, v in enumerate(success_rate.values):
        axes[0, 0].text(i, v + 0.02, f'{v:.2f}', ha='center', fontweight='bold')
    
    # 총점 분포
    axes[0, 1].hist([flap_scores['총점'], hijack_scores['총점'], loop_scores['총점']], 
                    bins=10, alpha=0.7, label=['FLAP', 'HIJACK', 'LOOP'], 
                    color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
    axes[0, 1].set_title('Total Score Distribution')
    axes[0, 1].set_xlabel('Total Score')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].legend()
    
    # 카테고리별 평균 점수
    categories = ['실행여부', '이벤트종류', '시간범위', '수치일치', '설명품질']
    category_means = all_scores.groupby('test_type')[categories].mean()
    
    x = np.arange(len(categories))
    width = 0.25
    
    for i, test_type in enumerate(['FLAP', 'HIJACK', 'LOOP']):
        axes[1, 0].bar(x + i*width, category_means.loc[test_type], width, 
                       label=test_type, alpha=0.8)
    
    axes[1, 0].set_title('Average Score by Category')
    axes[1, 0].set_xlabel('Categories')
    axes[1, 0].set_ylabel('Average Score')
    axes[1, 0].set_xticks(x + width)
    axes[1, 0].set_xticklabels(categories, rotation=45)
    axes[1, 0].legend()
    axes[1, 0].set_ylim(0, 3)
    
    # 박스플롯 - 총점 분포
    box_data = [flap_scores['총점'], hijack_scores['총점'], loop_scores['총점']]
    axes[1, 1].boxplot(box_data, labels=['FLAP', 'HIJACK', 'LOOP'])
    axes[1, 1].set_title('Total Score Distribution (Box Plot)')
    axes[1, 1].set_ylabel('Total Score')
    
    plt.tight_layout()
    plt.savefig('/app/eval/test_results_overview.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_detailed_analysis():
    """상세 분석 차트 생성"""
    # 데이터 로드
    flap_df = load_jsonl_data('/app/eval/flap/flap_graded_results.jsonl')
    hijack_df = load_jsonl_data('/app/eval/hijack/hijack_graded_results.jsonl')
    loop_df = load_jsonl_data('/app/eval/loop/loop_graded_results.jsonl')
    
    # 점수 데이터 추출
    flap_scores = extract_score_data(flap_df)
    hijack_scores = extract_score_data(hijack_df)
    loop_scores = extract_score_data(loop_df)
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Detailed Performance Analysis', fontsize=16, fontweight='bold')
    
    # 1. 각 테스트별 카테고리 점수 히트맵
    test_types = ['FLAP', 'HIJACK', 'LOOP']
    score_data = [flap_scores, hijack_scores, loop_scores]
    
    categories = ['실행여부', '이벤트종류', '시간범위', '수치일치', '설명품질']
    
    for i, (test_type, scores) in enumerate(zip(test_types, score_data)):
        if i < 3:
            row, col = i // 3, i % 3
            if i == 0:
                row, col = 0, 0
            elif i == 1:
                row, col = 0, 1
            else:
                row, col = 0, 2
            
            # 카테고리별 점수 히트맵
            heatmap_data = scores[categories].values
            sns.heatmap(heatmap_data.T, annot=True, fmt='.1f', cmap='RdYlGn', 
                       xticklabels=False, yticklabels=categories, ax=axes[row, col])
            axes[row, col].set_title(f'{test_type} Test - Category Scores')
    
    # 2. 성공/실패별 점수 분포
    all_scores = pd.concat([
        flap_scores.assign(test_type='FLAP'),
        hijack_scores.assign(test_type='HIJACK'),
        loop_scores.assign(test_type='LOOP')
    ], ignore_index=True)
    
    # 성공한 테스트들의 점수 분포
    success_scores = all_scores[all_scores['success'] == True]['총점']
    fail_scores = all_scores[all_scores['success'] == False]['총점']
    
    axes[1, 0].hist([success_scores, fail_scores], bins=8, alpha=0.7, 
                    label=['Success', 'Failure'], color=['#2ECC71', '#E74C3C'])
    axes[1, 0].set_title('Score Distribution: Success vs Failure')
    axes[1, 0].set_xlabel('Total Score')
    axes[1, 0].set_ylabel('Frequency')
    axes[1, 0].legend()
    
    # 3. 카테고리별 성능 비교
    category_performance = all_scores.groupby('test_type')[categories].mean()
    
    x = np.arange(len(categories))
    width = 0.25
    
    for i, test_type in enumerate(['FLAP', 'HIJACK', 'LOOP']):
        axes[1, 1].bar(x + i*width, category_performance.loc[test_type], width, 
                       label=test_type, alpha=0.8)
    
    axes[1, 1].set_title('Category Performance Comparison')
    axes[1, 1].set_xlabel('Categories')
    axes[1, 1].set_ylabel('Average Score')
    axes[1, 1].set_xticks(x + width)
    axes[1, 1].set_xticklabels(categories, rotation=45)
    axes[1, 1].legend()
    axes[1, 1].set_ylim(0, 3)
    
    # 4. 점수 상관관계 분석
    correlation_matrix = all_scores[categories + ['총점']].corr()
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                ax=axes[1, 2])
    axes[1, 2].set_title('Score Correlation Matrix')
    
    plt.tight_layout()
    plt.savefig('/app/eval/detailed_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_performance_summary():
    """성능 요약 테이블 생성"""
    # 데이터 로드
    flap_df = load_jsonl_data('/app/eval/flap/flap_graded_results.jsonl')
    hijack_df = load_jsonl_data('/app/eval/hijack/hijack_graded_results.jsonl')
    loop_df = load_jsonl_data('/app/eval/loop/loop_graded_results.jsonl')
    
    # 점수 데이터 추출
    flap_scores = extract_score_data(flap_df)
    hijack_scores = extract_score_data(hijack_df)
    loop_scores = extract_score_data(loop_df)
    
    # 요약 통계 생성
    summary_data = []
    
    for test_type, scores in [('FLAP', flap_scores), ('HIJACK', hijack_scores), ('LOOP', loop_scores)]:
        summary = {
            'Test Type': test_type,
            'Total Tests': len(scores),
            'Success Rate': f"{scores['success'].mean():.2%}",
            'Avg Total Score': f"{scores['총점'].mean():.2f}",
            'Avg Execution': f"{scores['실행여부'].mean():.2f}",
            'Avg Event Type': f"{scores['이벤트종류'].mean():.2f}",
            'Avg Time Range': f"{scores['시간범위'].mean():.2f}",
            'Avg Numeric Match': f"{scores['수치일치'].mean():.2f}",
            'Avg Explanation': f"{scores['설명품질'].mean():.2f}",
            'Max Score': scores['총점'].max(),
            'Min Score': scores['총점'].min(),
            'Std Dev': f"{scores['총점'].std():.2f}"
        }
        summary_data.append(summary)
    
    summary_df = pd.DataFrame(summary_data)
    
    # 테이블 시각화
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.axis('tight')
    ax.axis('off')
    
    table = ax.table(cellText=summary_df.values, colLabels=summary_df.columns,
                     cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # 헤더 스타일링
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#4CAF50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # 데이터 행 스타일링
    colors = ['#E8F5E8', '#F0F8F0', '#F8FBF8']
    for i in range(1, len(summary_df) + 1):
        for j in range(len(summary_df.columns)):
            table[(i, j)].set_facecolor(colors[i-1])
    
    plt.title('BGP Anomaly Detection Test Performance Summary', 
              fontsize=16, fontweight='bold', pad=20)
    plt.savefig('/app/eval/performance_summary.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    return summary_df

def create_score_distribution():
    """점수 분포 상세 분석"""
    # 데이터 로드
    flap_df = load_jsonl_data('/app/eval/flap/flap_graded_results.jsonl')
    hijack_df = load_jsonl_data('/app/eval/hijack/hijack_graded_results.jsonl')
    loop_df = load_jsonl_data('/app/eval/loop/loop_graded_results.jsonl')
    
    # 점수 데이터 추출
    flap_scores = extract_score_data(flap_df)
    hijack_scores = extract_score_data(hijack_df)
    loop_scores = extract_score_data(loop_df)
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Score Distribution Analysis', fontsize=16, fontweight='bold')
    
    # 1. 각 테스트별 점수 분포
    test_data = [
        (flap_scores['총점'], 'FLAP', '#FF6B6B'),
        (hijack_scores['총점'], 'HIJACK', '#4ECDC4'),
        (loop_scores['총점'], 'LOOP', '#45B7D1')
    ]
    
    for i, (scores, label, color) in enumerate(test_data):
        axes[0, 0].hist(scores, bins=8, alpha=0.6, label=label, color=color)
    axes[0, 0].set_title('Total Score Distribution by Test Type')
    axes[0, 0].set_xlabel('Total Score')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].legend()
    
    # 2. 카테고리별 점수 분포 (FLAP)
    categories = ['실행여부', '이벤트종류', '시간범위', '수치일치', '설명품질']
    flap_category_scores = [flap_scores[cat] for cat in categories]
    
    axes[0, 1].boxplot(flap_category_scores, labels=categories)
    axes[0, 1].set_title('FLAP Test - Category Score Distribution')
    axes[0, 1].set_ylabel('Score')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. 카테고리별 점수 분포 (HIJACK)
    hijack_category_scores = [hijack_scores[cat] for cat in categories]
    
    axes[1, 0].boxplot(hijack_category_scores, labels=categories)
    axes[1, 0].set_title('HIJACK Test - Category Score Distribution')
    axes[1, 0].set_ylabel('Score')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. 카테고리별 점수 분포 (LOOP)
    loop_category_scores = [loop_scores[cat] for cat in categories]
    
    axes[1, 1].boxplot(loop_category_scores, labels=categories)
    axes[1, 1].set_title('LOOP Test - Category Score Distribution')
    axes[1, 1].set_ylabel('Score')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('/app/eval/score_distribution.png', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    print("Creating BGP Anomaly Detection Test Results Visualization...")
    
    # 전체 개요 차트
    print("1. Creating overview charts...")
    create_overview_charts()
    
    # 상세 분석 차트
    print("2. Creating detailed analysis...")
    create_detailed_analysis()
    
    # 성능 요약 테이블
    print("3. Creating performance summary...")
    summary_df = create_performance_summary()
    print("\nPerformance Summary:")
    print(summary_df.to_string(index=False))
    
    # 점수 분포 분석
    print("4. Creating score distribution analysis...")
    create_score_distribution()
    
    print("\nVisualization complete! Generated files:")
    print("- test_results_overview.png")
    print("- detailed_analysis.png") 
    print("- performance_summary.png")
    print("- score_distribution.png")
