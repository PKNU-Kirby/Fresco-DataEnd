# 유통상품 표준DB 상품분류 소개 크롤링

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# URL 주소: https://www.allproductkorea.or.kr/products/database/category
driver.get("https://www.allproductkorea.or.kr/products/database/category")
wait = WebDriverWait(driver, 20)

result = []

# 대분류 ul
depth_1_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-1')))
depth_1_items = depth_1_ul.find_elements(By.TAG_NAME, 'li')

# 대분류 2개만 처리 (예: 0,1)
for i1 in range(min(2, len(depth_1_items))):
    li1 = depth_1_items[i1]
    a1 = li1.find_element(By.TAG_NAME, 'a')
    name1 = a1.find_element(By.CLASS_NAME, 'cls_nm').text

    # 대분류 클릭
    depth_1_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-1')))
    depth_1_items = depth_1_ul.find_elements(By.TAG_NAME, 'li')
    a1 = depth_1_items[i1].find_element(By.TAG_NAME, 'a')
    driver.execute_script("arguments[0].click();", a1)

    # 중분류 대기 및 수집
    depth_2_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-2')))
    time.sleep(0.5)
    depth_2_items = depth_2_ul.find_elements(By.TAG_NAME, 'li')

    if not depth_2_items:
        result.append((name1, None, None, None))
        continue

    for i2, li2 in enumerate(depth_2_items):
        a2 = li2.find_element(By.TAG_NAME, 'a')
        name2 = a2.find_element(By.CLASS_NAME, 'cls_nm').text

        # 중분류 클릭
        depth_2_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-2')))
        depth_2_items = depth_2_ul.find_elements(By.TAG_NAME, 'li')
        a2 = depth_2_items[i2].find_element(By.TAG_NAME, 'a')
        driver.execute_script("arguments[0].click();", a2)

        # 소분류 대기 및 수집
        depth_3_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-3')))
        time.sleep(0.5)
        depth_3_items = depth_3_ul.find_elements(By.TAG_NAME, 'li')

        if not depth_3_items:
            result.append((name1, name2, None, None))
            continue

        for i3, li3 in enumerate(depth_3_items):
            a3 = li3.find_element(By.TAG_NAME, 'a')
            name3 = a3.find_element(By.CLASS_NAME, 'cls_nm').text

            # 소분류 클릭
            depth_3_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-3')))
            depth_3_items = depth_3_ul.find_elements(By.TAG_NAME, 'li')
            a3 = depth_3_items[i3].find_element(By.TAG_NAME, 'a')
            driver.execute_script("arguments[0].click();", a3)

            # 세분류 대기 및 수집
            depth_4_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-4')))
            time.sleep(0.5)
            depth_4_items = depth_4_ul.find_elements(By.TAG_NAME, 'li')

            if not depth_4_items:
                result.append((name1, name2, name3, None))
                continue

            for li4 in depth_4_items:
                a4 = li4.find_element(By.TAG_NAME, 'a')
                name4 = a4.find_element(By.CLASS_NAME, 'cls_nm').text

                # 세분류 클릭
                depth_4_ul = wait.until(EC.presence_of_element_located((By.ID, 'depth-4')))
                depth_4_items = depth_4_ul.find_elements(By.TAG_NAME, 'li')
                a4 = depth_4_items[depth_4_items.index(li4)].find_element(By.TAG_NAME, 'a')
                driver.execute_script("arguments[0].click();", a4)

                # 세분류 클릭 후 'notEmpty' 영역 대기 및 예시 수집
                wait.until(EC.visibility_of_element_located((By.ID, 'notEmpty')))
                example = ''
                try:
                    example_elem = driver.find_element(By.ID, 'example')
                    example = example_elem.text.strip()
                except:
                    example = ''

                result.append((name1, name2, name3, name4, example))

# 결과 출력
for row in result:
    print(f"대분류: {row[0]}, 중분류: {row[1]}, 소분류: {row[2]}, 세분류: {row[3]}, 예시: {row[4]}")

csv_filename = "product_categories.csv"
final_result = []

for row in result:
    # row = (대분류명, 중분류명, 소분류명, 세분류명, 예시)
    examples = row[4]
    if examples:
        example_list = [e.strip() for e in examples.split(',')]
    else:
        example_list = ['']  # 예시가 없으면 빈 문자열
    
    for example in example_list:
        final_result.append((row[0], row[1], row[2], row[3], example))

# final_result를 csv로 저장
with open(csv_filename, mode='w', newline='', encoding='utf-8-sig') as f:
    writer = csv.writer(f)
    writer.writerow(['대분류명', '중분류명', '소분류명', '세분류명', '예시'])
    for r in final_result:
        writer.writerow(r)

print(f"{csv_filename} 파일로 저장 완료")
