Quick Start (กิ๊ฟรันเองในคอมได้เลย)

1) ติดตั้งไลบรารี (ครั้งแรกเท่านั้น)
   pip install pandas openpyxl pyyaml

2) โครงไฟล์
   /scripts/build_master.py
   /config/budget_mapping.yml
   (ไฟล์ข้อมูล) 1748328157_5318.xlsx  [มีชีท data]

3) สั่งรัน (ตัวอย่างใช้ไฟล์ปี 69 ที่อัปมาแล้ว)
   python scripts/build_master.py --input_xlsx 1748328157_5318.xlsx --sheet data --config_yml config/budget_mapping.yml --out_dir output

4) ผลลัพธ์
   output/MasterData.xlsx   (พร้อม Pivot ต่อได้)
   output/DataIssues.xlsx   (แถวที่จัดกลุ่มไม่ได้/ยอดว่าง)

หมายเหตุ
- แก้ไข logic การจัดหมวด/แผน/ประจำ-ลงทุน ได้ที่ config/budget_mapping.yml
- ใช้ได้กับกระทรวง/กรม อื่น ๆ และปีถัดไป เพราะเป็นแบบ config-driven
- ถ้าปีถัดไปมีเฉพาะ "ช่องคำขอ" (ยังไม่มีข้อเสนอ) ให้ตั้งค่าชื่อคอลัมน์ให้ตรงหรือเติมคอลัมน์ว่างใน Excel ก่อนรัน (สคริปต์จะไม่ล้ม แต่จะจัดเข้าหมวด 'ไม่ทราบหมวด' ถ้าหาคีย์เวิร์ดไม่เจอ)
