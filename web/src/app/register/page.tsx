"use client";

import { useEffect, useRef } from "react";
import { LoaderCircle } from "lucide-react";

import { PageHeader } from "@/components/page-header";
import webConfig from "@/constants/common-env";
import { useAuthGuard } from "@/lib/use-auth-guard";
import type { RegisterConfig } from "@/lib/api";
import { getStoredSessionToken } from "@/store/auth";

import { useSettingsStore } from "../settings/store";
import { RegisterCard } from "./components/register-card";

function RegisterDataController() {
  const didLoadRef = useRef(false);
  const loadRegister = useSettingsStore((state) => state.loadRegister);
  const setRegisterConfig = useSettingsStore((state) => state.setRegisterConfig);
  const updateRegisterStatsAndLogs = useSettingsStore((state) => state.updateRegisterStatsAndLogs);

  useEffect(() => {
    if (didLoadRef.current) return;
    didLoadRef.current = true;
    void loadRegister();
  }, [loadRegister]);

  useEffect(() => {
    let source: EventSource | null = null;
    let closed = false;
    void getStoredSessionToken().then((token) => {
      if (closed || !token) return;
      const baseUrl = webConfig.apiUrl.replace(/\/$/, "");
      source = new EventSource(`${baseUrl}/api/register/events?token=${encodeURIComponent(token)}`);
      source.onmessage = (event) => {
        const data = JSON.parse(event.data) as RegisterConfig;
        // SSE 只更新运行时状态（stats、logs、enabled），不覆盖用户正在编辑的配置字段
        updateRegisterStatsAndLogs(data);
      };
    });
    return () => {
      closed = true;
      source?.close();
    };
  }, [setRegisterConfig, updateRegisterStatsAndLogs]);

  return null;
}

function RegisterPageContent() {
  return (
    <>
      <RegisterDataController />
      <PageHeader eyebrow="Register" title="ChatGPT注册机" />
      <section className="mt-5">
        <RegisterCard />
      </section>
    </>
  );
}

export default function RegisterPage() {
  const { isCheckingAuth, session } = useAuthGuard(undefined, "/register");

  if (isCheckingAuth || !session) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <LoaderCircle className="size-5 animate-spin text-stone-400" />
      </div>
    );
  }

  return <RegisterPageContent />;
}
