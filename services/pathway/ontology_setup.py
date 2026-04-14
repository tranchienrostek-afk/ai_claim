import os
from neo4j import GraphDatabase
from runtime_env import load_notebooklm_env

load_notebooklm_env()

class OntologySetup:
    def __init__(self):
        self.uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
        self.user = os.getenv("neo4j_user", "neo4j")
        self.password = os.getenv("neo4j_password", "password123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def run_commands(self):
        commands = [
            # --- Tầng 1: Upper Ontology ---
            """
            MERGE (c_benh:OntologyClass {uri: "onto:Disease", label: "Bệnh Lý"})
            MERGE (c_thuoc:OntologyClass {uri: "onto:Drug", label: "Dược Phẩm"})
            MERGE (c_hoatchat:OntologyClass {uri: "onto:ActiveIngredient", label: "Hoạt Chất"})
            MERGE (c_trieuchung:OntologyClass {uri: "onto:Symptom", label: "Triệu Chứng"})
            MERGE (c_xetnghiem:OntologyClass {uri: "onto:LabTest", label: "Xét Nghiệm"})
            """,
            """
            MERGE (p_dieutri:ObjectProperty {uri: "onto:treats", label: "Điều trị"})
            MERGE (p_chongchidinh:ObjectProperty {uri: "onto:contraindicated_for", label: "Chống chỉ định"})
            MERGE (p_baogom:ObjectProperty {uri: "onto:contains_ingredient", label: "Chứa hoạt chất"})
            """,
            """
            MATCH (p_dieutri:ObjectProperty {uri: "onto:treats"}), 
                  (c_thuoc:OntologyClass {uri: "onto:Drug"}),
                  (c_benh:OntologyClass {uri: "onto:Disease"})
            MERGE (p_dieutri)-[:DOMAIN]->(c_thuoc)
            MERGE (p_dieutri)-[:RANGE]->(c_benh)
            """,
            
            # --- Tầng 2: Domain Ontology (ICD-10 & ATC) ---
            """
            MERGE (chuong_6:ICD_Chapter {code: "VI", name: "Bệnh hệ thần kinh (G00-G99)"})
            MERGE (nhom_g40_g47:ICD_Block {code: "G40-G47", name: "Rối loạn từng cơn và kịch phát"})
            MERGE (benh_g47:ICD_Category {code: "G47", name: "Rối loạn giấc ngủ"})
            MERGE (benh_g47)-[:IS_A]->(nhom_g40_g47)
            MERGE (nhom_g40_g47)-[:IS_A]->(chuong_6)
            """,
            """
            MATCH (chuong_6:ICD_Chapter {code: "VI"}), (c_benh:OntologyClass {uri: "onto:Disease"})
            MERGE (chuong_6)-[:INSTANCE_OF]->(c_benh)
            """,
            """
            MERGE (nhom_n:ATC_Level1 {code: "N", name: "Hệ Thần Kinh"})
            MERGE (nhom_n05:ATC_Level2 {code: "N05", name: "Thuốc an thần"})
            MERGE (nhom_n05b:ATC_Level3 {code: "N05B", name: "Thuốc giải lo âu"})
            MERGE (thuoc_diazepam:DrugConcept {code: "N05BA01", name: "Diazepam"})
            MERGE (thuoc_diazepam)-[:IS_A]->(nhom_n05b)
            MERGE (nhom_n05b)-[:IS_A]->(nhom_n05)
            MERGE (nhom_n05)-[:IS_A]->(nhom_n)
            """,
            """
            MATCH (thuoc_diazepam:DrugConcept {code: "N05BA01"}), (benh_g47:ICD_Category {code: "G47"})
            MERGE (thuoc_diazepam)-[:TREATS]->(benh_g47)
            """,

            # --- Tầng 2b: TCM Ontology Classes ---
            """
            MERGE (c_syndrome:OntologyClass {uri: "onto:TCMSyndrome", label: "Thể bệnh YHCT"})
            MERGE (c_formula:OntologyClass {uri: "onto:HerbalFormula", label: "Bài thuốc"})
            MERGE (c_acupoint:OntologyClass {uri: "onto:AcupuncturePoint", label: "Huyệt châm cứu"})
            MERGE (c_treatment:OntologyClass {uri: "onto:TreatmentMethod", label: "Phương pháp điều trị"})
            """,

            # --- Tầng 2c: ICD F51 (Insomnia - Mental/Behavioral) ---
            """
            MERGE (chuong_5:ICD_Chapter {code: "V", name: "Rối loạn tâm thần và hành vi (F00-F99)"})
            MERGE (nhom_f50_f59:ICD_Block {code: "F50-F59", name: "Hội chứng hành vi kết hợp rối loạn sinh lý"})
            MERGE (benh_f51:ICD_Category {code: "F51", name: "Rối loạn giấc ngủ không thực tổn"})
            MERGE (benh_f51)-[:IS_A]->(nhom_f50_f59)
            MERGE (nhom_f50_f59)-[:IS_A]->(chuong_5)
            WITH benh_f51
            MATCH (c_benh:OntologyClass {uri: "onto:Disease"})
            MERGE (chuong_5)-[:INSTANCE_OF]->(c_benh)
            """,

            # --- Tầng 2d: TCM Syndrome Instances ---
            """
            MERGE (s1:SyndromeConcept {code: "YHCT_S01", name: "Tâm tỳ lưỡng hư", bat_cuong: "hư chứng"})
            MERGE (s2:SyndromeConcept {code: "YHCT_S02", name: "Âm hư hỏa vượng", bat_cuong: "hư chứng, nhiệt"})
            MERGE (s3:SyndromeConcept {code: "YHCT_S03", name: "Tâm đởm khí hư", bat_cuong: "hư chứng"})
            MERGE (s4:SyndromeConcept {code: "YHCT_S04", name: "Đàm nhiệt nội nhiễu", bat_cuong: "thực chứng, nhiệt"})
            MERGE (s5:SyndromeConcept {code: "YHCT_S05", name: "Can uất hóa hỏa", bat_cuong: "thực chứng, nhiệt"})
            WITH s1, s2, s3, s4, s5
            MATCH (c_syndrome:OntologyClass {uri: "onto:TCMSyndrome"})
            MERGE (s1)-[:INSTANCE_OF]->(c_syndrome)
            MERGE (s2)-[:INSTANCE_OF]->(c_syndrome)
            MERGE (s3)-[:INSTANCE_OF]->(c_syndrome)
            MERGE (s4)-[:INSTANCE_OF]->(c_syndrome)
            MERGE (s5)-[:INSTANCE_OF]->(c_syndrome)
            """,

            # --- Tầng 2e: Herbal Formula Instances ---
            """
            MERGE (f1:FormulaConcept {code: "YHCT_F01", name: "Quy tỳ thang"})
            MERGE (f2:FormulaConcept {code: "YHCT_F02", name: "Hoàng liên a giao thang"})
            MERGE (f3:FormulaConcept {code: "YHCT_F03", name: "An thần định chí hoàn"})
            MERGE (f4:FormulaConcept {code: "YHCT_F04", name: "Ôn đởm thang"})
            MERGE (f5:FormulaConcept {code: "YHCT_F05", name: "Long đởm tả can thang"})
            MERGE (f6:FormulaConcept {code: "YHCT_F06", name: "Thiên vương bổ tâm đan"})
            MERGE (f7:FormulaConcept {code: "YHCT_F07", name: "Toan táo nhân thang"})
            MERGE (f8:FormulaConcept {code: "YHCT_F08", name: "Chu sa an thần hoàn"})
            MERGE (f9:FormulaConcept {code: "YHCT_F09", name: "Gia vị tiêu dao tán"})
            WITH f1, f2, f3, f4, f5, f6, f7, f8, f9
            MATCH (c_formula:OntologyClass {uri: "onto:HerbalFormula"})
            MERGE (f1)-[:INSTANCE_OF]->(c_formula)
            MERGE (f2)-[:INSTANCE_OF]->(c_formula)
            MERGE (f3)-[:INSTANCE_OF]->(c_formula)
            MERGE (f4)-[:INSTANCE_OF]->(c_formula)
            MERGE (f5)-[:INSTANCE_OF]->(c_formula)
            MERGE (f6)-[:INSTANCE_OF]->(c_formula)
            MERGE (f7)-[:INSTANCE_OF]->(c_formula)
            MERGE (f8)-[:INSTANCE_OF]->(c_formula)
            MERGE (f9)-[:INSTANCE_OF]->(c_formula)
            """,

            # --- Tầng 2f: Formula ↔ Syndrome TREATS links ---
            """
            MATCH (f1:FormulaConcept {code: "YHCT_F01"}), (s1:SyndromeConcept {code: "YHCT_S01"})
            MERGE (f1)-[:TREATS]->(s1)
            WITH 1 as dummy
            MATCH (f2:FormulaConcept {code: "YHCT_F02"}), (s2:SyndromeConcept {code: "YHCT_S02"})
            MERGE (f2)-[:TREATS]->(s2)
            WITH 1 as dummy
            MATCH (f3:FormulaConcept {code: "YHCT_F03"}), (s3:SyndromeConcept {code: "YHCT_S03"})
            MERGE (f3)-[:TREATS]->(s3)
            WITH 1 as dummy
            MATCH (f4:FormulaConcept {code: "YHCT_F04"}), (s4:SyndromeConcept {code: "YHCT_S04"})
            MERGE (f4)-[:TREATS]->(s4)
            WITH 1 as dummy
            MATCH (f5:FormulaConcept {code: "YHCT_F05"}), (s5:SyndromeConcept {code: "YHCT_S05"})
            MERGE (f5)-[:TREATS]->(s5)
            WITH 1 as dummy
            MATCH (f6:FormulaConcept {code: "YHCT_F06"}), (s2b:SyndromeConcept {code: "YHCT_S02"})
            MERGE (f6)-[:TREATS]->(s2b)
            WITH 1 as dummy
            MATCH (f7:FormulaConcept {code: "YHCT_F07"}), (s1b:SyndromeConcept {code: "YHCT_S01"})
            MERGE (f7)-[:TREATS]->(s1b)
            WITH 1 as dummy
            MATCH (f9:FormulaConcept {code: "YHCT_F09"}), (s5b:SyndromeConcept {code: "YHCT_S05"})
            MERGE (f9)-[:TREATS]->(s5b)
            """,

            # --- Tầng 2g: Syndrome → Disease cross-links ---
            """
            MATCH (s:SyndromeConcept), (g47:ICD_Category {code: "G47"})
            MERGE (s)-[:YHCT_DIAGNOSIS_FOR]->(g47)
            WITH s
            MATCH (f51:ICD_Category {code: "F51"})
            MERGE (s)-[:YHCT_DIAGNOSIS_FOR]->(f51)
            """,

            # --- Tầng 2h: Additional Western drugs for insomnia ---
            """
            MERGE (thuoc_piracetam:DrugConcept {code: "N06BX03", name: "Piracetam"})
            MERGE (thuoc_rotundin:DrugConcept {code: "N05CM_VN01", name: "Rotundin"})
            MERGE (thuoc_tanakan:DrugConcept {code: "N06DX_VN01", name: "Tanakan"})
            WITH thuoc_piracetam, thuoc_rotundin, thuoc_tanakan
            MATCH (benh_g47:ICD_Category {code: "G47"})
            MERGE (thuoc_piracetam)-[:TREATS]->(benh_g47)
            MERGE (thuoc_rotundin)-[:TREATS]->(benh_g47)
            MERGE (thuoc_tanakan)-[:TREATS]->(benh_g47)
            WITH thuoc_piracetam, thuoc_rotundin, thuoc_tanakan
            MATCH (benh_f51:ICD_Category {code: "F51"})
            MERGE (thuoc_piracetam)-[:TREATS]->(benh_f51)
            MERGE (thuoc_rotundin)-[:TREATS]->(benh_f51)
            MERGE (thuoc_tanakan)-[:TREATS]->(benh_f51)
            """,

            # --- Tầng 3: Task Ontology (Rules Engine) ---
            """
            MERGE (rule_1:ClinicalRule {
                rule_id: "R_DIAZEPAM_G47",
                name: "Chỉ định Diazepam cho Rối loạn giấc ngủ",
                description: "Bệnh nhân phải có chẩn đoán G47 và trên 18 tuổi mới được kê Diazepam 5mg",
                severity: "HIGH"
            })
            MERGE (cond_1:Condition {
                type: "HAS_DIAGNOSIS",
                operator: "IN_HIERARCHY",
                value: "G47"
            })
            MERGE (cond_2:Condition {
                type: "PATIENT_AGE",
                operator: ">=",
                value: 18
            })
            MERGE (action_1:Action {
                type: "ALLOW_PRESCRIPTION",
                target_code: "N05BA01"
            })
            MERGE (rule_1)-[:REQUIRES_CONDITION {logic: "AND"}]->(cond_1)
            MERGE (rule_1)-[:REQUIRES_CONDITION {logic: "AND"}]->(cond_2)
            MERGE (rule_1)-[:YIELDS_ACTION]->(action_1)
            """
        ]

        with self.driver.session() as session:
            for i, cmd in enumerate(commands):
                print(f"Executing Ontology Step {i+1}...")
                session.run(cmd)
        print("Ontology Setup Complete.")

if __name__ == "__main__":
    setup = OntologySetup()
    try:
        setup.run_commands()
    finally:
        setup.close()
