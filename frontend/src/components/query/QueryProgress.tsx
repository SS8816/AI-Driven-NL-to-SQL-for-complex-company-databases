import { CheckCircle2, Loader2, XCircle } from 'lucide-react';
import { Card } from '@/components/common';

export interface ProgressStep {
  stage: string;
  message: string;
  progress_percent?: number;
  timestamp: string;
  status: 'running' | 'completed' | 'failed';
}

interface QueryProgressProps {
  steps: ProgressStep[];
  currentStep?: string;
}

export function QueryProgress({ steps, currentStep }: QueryProgressProps) {
  return (
    <Card title="Execution Progress" subtitle="Real-time query execution status">
      <div className="space-y-3">
        {steps.map((step, index) => {
          const isCurrentStep = step.stage === currentStep;
          const isCompleted = step.status === 'completed';
          const isFailed = step.status === 'failed';

          return (
            <div
              key={index}
              className="flex items-start gap-3 p-3 bg-dark-sidebar rounded-lg"
            >
              <div className="flex-shrink-0 mt-0.5">
                {isFailed ? (
                  <XCircle className="w-5 h-5 text-error" />
                ) : isCompleted ? (
                  <CheckCircle2 className="w-5 h-5 text-success" />
                ) : (
                  <Loader2 className="w-5 h-5 text-primary-500 animate-spin" />
                )}
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2 mb-1">
                  <span
                    className={`text-sm font-medium ${
                      isCurrentStep
                        ? 'text-primary-400'
                        : isCompleted
                        ? 'text-success'
                        : isFailed
                        ? 'text-error'
                        : 'text-gray-400'
                    }`}
                  >
                    {step.stage}
                  </span>
                  {step.progress_percent !== undefined && (
                    <span className="text-xs text-gray-500">
                      {step.progress_percent}%
                    </span>
                  )}
                </div>
                <p className="text-sm text-gray-400">{step.message}</p>

                {step.progress_percent !== undefined && (
                  <div className="mt-2 w-full bg-dark-border rounded-full h-1.5">
                    <div
                      className="bg-primary-500 h-1.5 rounded-full transition-all duration-300"
                      style={{ width: `${step.progress_percent}%` }}
                    />
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
