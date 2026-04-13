import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { 
  UploadCloud, 
  BrainCircuit, 
  FileCheck2, 
  ArrowRight, 
  CheckCircle2, 
  Loader2, 
  Activity,
  AlertCircle
} from "lucide-react";

export default function StoryDrivenDashboard() {
  // Quản lý luồng bằng các bước (Step)
  const [currentStep, setCurrentStep] = useState(1);
  const [isProcessing, setIsProcessing] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);

  // Giả lập xử lý reasoning
  const handleRunReasoning = () => {
    setIsProcessing(true);
    setTimeout(() => {
      setIsProcessing(false);
      setCurrentStep(3);
    }, 2500);
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 font-sans selection:bg-blue-200">
      {/* Header gọn nhẹ, ẩn bớt các thông số hạ tầng không cần thiết */}
      <header className="bg-white border-b border-slate-200 px-6 py-4 sticky top-0 z-10">
        <div className="max-w-5xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-blue-600 p-2 rounded-xl text-white">
              <BrainCircuit className="h-5 w-5" />
            </div>
            <div>
              <h1 className="font-semibold text-lg text-slate-800">AI Claim Engineer</h1>
              <p className="text-xs text-slate-500">Hệ thống thẩm định & Reasoning</p>
            </div>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 bg-emerald-50 text-emerald-700 rounded-full border border-emerald-100 text-sm font-medium">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
            </span>
            System Online
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto py-12 px-6">
        {/* Thanh tiến trình kể câu chuyện */}
        <div className="mb-12">
          <div className="flex items-center justify-between relative">
            <div className="absolute left-0 top-1/2 -translate-y-1/2 w-full h-1 bg-slate-200 rounded-full -z-10"></div>
            <div className={`absolute left-0 top-1/2 -translate-y-1/2 h-1 bg-blue-600 rounded-full transition-all duration-500 -z-10`} style={{ width: currentStep === 1 ? '0%' : currentStep === 2 ? '50%' : '100%' }}></div>
            
            <StepIndicator step={1} currentStep={currentStep} icon={UploadCloud} label="Nạp dữ liệu" />
            <StepIndicator step={2} currentStep={currentStep} icon={BrainCircuit} label="AI Phân tích" />
            <StepIndicator step={3} currentStep={currentStep} icon={FileCheck2} label="Kết quả Audit" />
          </div>
        </div>

        {/* Nội dung chính dựa trên từng bước */}
        <div className="bg-white rounded-3xl shadow-sm border border-slate-200 p-8 md:p-12 min-h-[400px] relative overflow-hidden">
          <AnimatePresence mode="wait">
            
            {/* BƯỚC 1: TẬP TRUNG VÀO INPUT */}
            {currentStep === 1 && (
              <motion.div 
                key="step1"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="max-w-xl mx-auto text-center space-y-8"
              >
                <div>
                  <h2 className="text-2xl font-bold text-slate-800">Bắt đầu phiên thẩm định mới</h2>
                  <p className="text-slate-500 mt-2">Vui lòng cung cấp hồ sơ hoặc tải lên file case (JSON/PDF) để hệ thống AI bắt đầu đọc hiểu và xử lý.</p>
                </div>

                <div className="border-2 border-dashed border-slate-300 rounded-2xl p-10 hover:bg-slate-50 hover:border-blue-400 transition-colors cursor-pointer group">
                  <UploadCloud className="mx-auto h-12 w-12 text-slate-400 group-hover:text-blue-500 mb-4" />
                  <p className="font-medium text-slate-700">Kéo thả file vào đây hoặc <span className="text-blue-600">tải lên từ máy</span></p>
                  <p className="text-xs text-slate-400 mt-2">Hỗ trợ: .json, .pdf, .docx (Tối đa 50MB)</p>
                </div>

                <div className="flex justify-center">
                  <button 
                    onClick={() => setCurrentStep(2)}
                    className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-3 rounded-xl font-medium flex items-center gap-2 transition-all shadow-sm shadow-blue-200"
                  >
                    Tiếp tục cấu hình <ArrowRight className="h-4 w-4" />
                  </button>
                </div>
              </motion.div>
            )}

            {/* BƯỚC 2: CẤU HÌNH VÀ CHẠY REASONING */}
            {currentStep === 2 && (
              <motion.div 
                key="step2"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="max-w-2xl mx-auto space-y-8"
              >
                <div className="text-center">
                  <h2 className="text-2xl font-bold text-slate-800">Xác nhận luồng Reasoning</h2>
                  <p className="text-slate-500 mt-2">Hệ thống sẽ đồng bộ dữ liệu vào Pathway và bắt đầu quá trình đối chiếu tri thức y khoa.</p>
                </div>

                <div className="bg-slate-50 rounded-2xl p-6 border border-slate-200 space-y-4">
                  <div className="flex justify-between items-center pb-4 border-b border-slate-200">
                    <span className="text-slate-600 font-medium">Tệp dữ liệu đầu vào</span>
                    <span className="text-slate-900">sample_case_meniere.json</span>
                  </div>
                  <div className="flex justify-between items-center pb-4 border-b border-slate-200">
                    <span className="text-slate-600 font-medium">Target Root Graph</span>
                    <span className="text-slate-900">protocols $\rightarrow$ neo4j_main</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-slate-600 font-medium">Auto Ingest Bridge</span>
                    <span className="bg-emerald-100 text-emerald-700 px-3 py-1 rounded-full text-xs font-semibold">Bật</span>
                  </div>
                </div>

                <div className="flex justify-between items-center pt-4">
                  <button 
                    onClick={() => setCurrentStep(1)}
                    className="text-slate-500 hover:text-slate-800 font-medium px-4 py-2"
                  >
                    Quay lại
                  </button>
                  <button 
                    onClick={handleRunReasoning}
                    disabled={isProcessing}
                    className="bg-slate-900 hover:bg-slate-800 text-white px-8 py-3 rounded-xl font-medium flex items-center gap-2 transition-all disabled:opacity-70"
                  >
                    {isProcessing ? (
                      <><Loader2 className="h-4 w-4 animate-spin" /> Đang chạy suy luận...</>
                    ) : (
                      <><Activity className="h-4 w-4" /> Kích hoạt Agent</>
                    )}
                  </button>
                </div>
              </motion.div>
            )}

            {/* BƯỚC 3: HIỂN THỊ KẾT QUẢ RÕ RÀNG */}
            {currentStep === 3 && (
              <motion.div 
                key="step3"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="space-y-8"
              >
                <div className="flex flex-col items-center text-center space-y-3">
                  <div className="h-16 w-16 bg-emerald-100 rounded-full flex items-center justify-center text-emerald-600 mb-2">
                    <CheckCircle2 className="h-8 w-8" />
                  </div>
                  <h2 className="text-2xl font-bold text-slate-800">Hoàn tất quá trình thẩm định</h2>
                  <p className="text-slate-500 max-w-lg">Hệ thống đã so sánh xong kết quả của Agent và Benchmark. Dưới đây là tóm tắt nhanh, bạn có thể xem Audit Log để biết chi tiết từng node trên Graph.</p>
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <div className="border border-slate-200 rounded-2xl p-6 bg-white">
                    <h3 className="font-semibold text-slate-800 mb-4 flex items-center gap-2">
                      <AlertCircle className="h-4 w-4 text-amber-500" /> Phát hiện sai lệch
                    </h3>
                    <p className="text-sm text-slate-600 mb-2">Agent đề xuất bồi thường mức B, tuy nhiên đối chiếu Knowledge Surface cho thấy chứng Meniere (H81.0) thuộc mức C.</p>
                    <button className="text-blue-600 text-sm font-medium hover:underline">Truy vết đồ thị (Trace) $\rightarrow$</button>
                  </div>

                  <div className="border border-slate-200 rounded-2xl p-6 bg-slate-900 text-white">
                    <h3 className="font-semibold text-slate-100 mb-4">Raw JSON Output</h3>
                    <pre className="text-xs text-slate-400 font-mono overflow-auto max-h-[100px] bg-black/50 p-3 rounded-lg border border-slate-700">
{`{
  "status": "completed",
  "confidence_score": 0.87,
  "flagged_nodes": ["H81_0", "policy_rule_4A"],
  "reasoning_time": "1.24s"
}`}
                    </pre>
                  </div>
                </div>

                <div className="flex justify-center pt-6">
                  <button 
                    onClick={() => setCurrentStep(1)}
                    className="bg-slate-100 hover:bg-slate-200 text-slate-800 px-6 py-2 rounded-xl font-medium transition-colors"
                  >
                    Chạy hồ sơ mới
                  </button>
                </div>
              </motion.div>
            )}

          </AnimatePresence>
        </div>
      </main>
    </div>
  );
}

// Component phụ trợ cho thanh trạng thái
function StepIndicator({ step, currentStep, icon: Icon, label }) {
  const isActive = currentStep >= step;
  const isCurrent = currentStep === step;

  return (
    <div className="flex flex-col items-center gap-2 z-10 relative">
      <div className={`h-10 w-10 rounded-full flex items-center justify-center transition-all duration-300 ${isActive ? 'bg-blue-600 text-white shadow-md shadow-blue-200' : 'bg-white border-2 border-slate-200 text-slate-400'}`}>
        <Icon className="h-5 w-5" />
      </div>
      <span className={`text-sm font-medium absolute -bottom-6 whitespace-nowrap ${isCurrent ? 'text-blue-700' : 'text-slate-500'}`}>
        {label}
      </span>
    </div>
  );
}