package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ============================================================================
// SECCIÓN 1: VARIABLES GLOBALES Y CONFIGURACIÓN
// ============================================================================

// requestCount lleva la cuenta de cuántas peticiones atendió ESTE Pod en particular.
// Se usa uint64 junto con sync/atomic para evitar condiciones de carrera (race conditions)
// si llegan varias peticiones al mismo tiempo.
var requestCount uint64

// Nombre del Deployment definido en k8s-manifest.yaml sobre el cual se consultan
// los pods y se aplican los cambios de escalado.
const deploymentName = "app-distribuidos"

// ============================================================================
// SECCIÓN 2: COMUNICACIÓN CON LA API INTERNA DE KUBERNETES
// ============================================================================

// k8sRequest realiza una petición HTTP a la API interna de Kubernetes desde adentro del Pod.
// Todo Pod en Kubernetes tiene montado automáticamente un token de autenticación en:
// /var/run/secrets/kubernetes.io/serviceaccount/token
// Gracias a los permisos RBAC (Role y RoleBinding en k8s-manifest.yaml), este Pod puede
// consultar la lista de pods, escalar el deployment o pedir que se elimine un pod.
func k8sRequest(method, path string, body []byte, contentType string) ([]byte, int, error) {
	// 1. Leer el token de seguridad inyectado por Kubernetes en el contenedor
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, 0, fmt.Errorf("no se pudo leer el token de K8s (¿corriendo fuera del clúster?): %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))

	// 2. Configurar el cliente HTTP para hablar por HTTPS con el API Server interno
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// "https://kubernetes.default.svc" es la dirección DNS interna del API Server del clúster
	url := "https://kubernetes.default.svc" + path
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}

	// 3. Armar la petición agregando el header Authorization: Bearer <token>
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	// 4. Ejecutar la petición y devolver el cuerpo de la respuesta y el código HTTP
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	return respBytes, resp.StatusCode, err
}

// getNamespace devuelve el namespace actual donde corre el Pod (por defecto "default").
func getNamespace() string {
	nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "default"
	}
	return strings.TrimSpace(string(nsBytes))
}

// PodInfo representa los datos básicos de cada Pod que enviamos al navegador (Frontend).
type PodInfo struct {
	Name   string `json:"name"`   // Nombre único del Pod (ej: app-distribuidos-xxx)
	Status string `json:"status"` // Estado actual: Running, Pending, Terminating, etc.
	Ready  bool   `json:"ready"`  // Indica si el contenedor ya está listo para recibir tráfico
}

// getClusterState consulta a Kubernetes cuántas réplicas están configuradas en el Deployment
// y cuál es la lista actual de Pods con sus respectivos estados.
func getClusterState() (int, []PodInfo) {
	ns := getNamespace()
	replicas := 1

	// 1. Consultar el subrecurso /scale del Deployment para saber cuántas réplicas deseadas hay
	scaleBytes, code, err := k8sRequest("GET", fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale", ns, deploymentName), nil, "")
	if err == nil && code == 200 {
		var scaleObj struct {
			Spec struct {
				Replicas int `json:"replicas"`
			} `json:"spec"`
		}
		if json.Unmarshal(scaleBytes, &scaleObj) == nil {
			replicas = scaleObj.Spec.Replicas
		}
	}

	// 2. Consultar todos los Pods que tengan el label "app=app-distribuidos"
	podsBytes, code, err := k8sRequest("GET", fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=app=%s", ns, deploymentName), nil, "")
	if err != nil || code != 200 {
		// Si falla (por ejemplo, si se ejecuta fuera de Kubernetes), devolvemos al menos el nodo actual
		hostname, _ := os.Hostname()
		return replicas, []PodInfo{{Name: hostname, Status: "Running", Ready: true}}
	}

	// Estructura para decodificar el JSON que devuelve la API de Kubernetes
	var podList struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				DeletionTimestamp *string `json:"deletionTimestamp"` // Si no es nil, el Pod se está borrando (Terminating)
			} `json:"metadata"`
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					Ready bool `json:"ready"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}

	var pods []PodInfo
	if json.Unmarshal(podsBytes, &podList) == nil {
		for _, item := range podList.Items {
			status := item.Status.Phase
			// Si Kubernetes marcó el Pod con fecha de borrado, lo mostramos como "Terminating"
			if item.Metadata.DeletionTimestamp != nil {
				status = "Terminating"
			}
			ready := false
			for _, cs := range item.Status.ContainerStatuses {
				if cs.Ready {
					ready = true
				}
			}
			pods = append(pods, PodInfo{
				Name:   item.Metadata.Name,
				Status: status,
				Ready:  ready,
			})
		}
	}
	return replicas, pods
}

// ============================================================================
// SECCIÓN 3: ENDPOINTS HTTP (API PARA EL FRONTEND Y CURL)
// ============================================================================

// pingHandler (/api/ping):
// Es el endpoint principal para probar el balanceo de carga desde el navegador.
// Cada vez que se lo llama, incrementa el contador de ESTE Pod y devuelve su nombre (hostname),
// cuántas peticiones lleva atendidas y el estado general del clúster.
func pingHandler(w http.ResponseWriter, r *http.Request) {
	// "Connection: close" evita que el navegador reutilice la misma conexión TCP,
	// obligando al Service de Kubernetes a balancear cada petición entre distintos Pods.
	w.Header().Set("Connection", "close")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	// Incrementar de forma segura el contador de peticiones de este Pod
	count := atomic.AddUint64(&requestCount, 1)

	// En Kubernetes, el Hostname de un contenedor es el nombre del Pod
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "desconocido"
	}

	replicas, pods := getClusterState()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"pod":      hostname,
		"count":    count,
		"replicas": replicas,
		"pods":     pods,
		"time":     time.Now().Format("15:04:05"),
	})
}

// statusHandler (/api/status):
// Devuelve la lista de Pods y cantidad de réplicas SIN incrementar el contador de tráfico.
// El navegador lo llama cada 2 segundos para refrescar la pantalla (ver si un Pod se creó o apagó).
func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	replicas, pods := getClusterState()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"replicas": replicas,
		"pods":     pods,
	})
}

// scaleHandler (/api/scale):
// Recibe por POST la nueva cantidad de réplicas deseada (ej: {"replicas": 4})
// y le envía un PATCH a la API de Kubernetes para escalar el Deployment en vivo.
func scaleHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Método no permitido"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Replicas int `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"JSON inválido"}`, http.StatusBadRequest)
		return
	}
	// Limitar entre 1 y 10 réplicas por seguridad para la demo
	if req.Replicas < 1 {
		req.Replicas = 1
	}
	if req.Replicas > 10 {
		req.Replicas = 10
	}

	ns := getNamespace()
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, req.Replicas)
	respBytes, code, err := k8sRequest(
		"PATCH",
		fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale", ns, deploymentName),
		[]byte(patch),
		"application/merge-patch+json",
	)
	if err != nil || code >= 300 {
		http.Error(w, fmt.Sprintf(`{"error":"No se pudo escalar (%d): %v - %s"}`, code, err, string(respBytes)), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":       true,
		"replicas": req.Replicas,
	})
}

// killHandler (/api/kill):
// Simula la caída o apagado de un nodo (Pod) específico recibiendo {"pod": "nombre-del-pod"}.
// Llama a DELETE en la API de Kubernetes sobre ese Pod con gracePeriodSeconds=1 para que muera rápido.
// Inmediatamente después, el ReplicaSet de Kubernetes detecta que falta un Pod y crea uno nuevo (Self-Healing).
func killHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Método no permitido"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Pod string `json:"pod"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	hostname, _ := os.Hostname()
	targetPod := req.Pod
	if targetPod == "" {
		targetPod = hostname
	}

	ns := getNamespace()
	_, code, err := k8sRequest("DELETE", fmt.Sprintf("/api/v1/namespaces/%s/pods/%s?gracePeriodSeconds=1", ns, targetPod), nil, "")
	if err != nil || code >= 300 {
		// Fallback: si no hay permisos de K8s o falla la API, apagamos el proceso actual con os.Exit(1)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":  true,
			"pod": hostname,
		})
		go func() {
			time.Sleep(200 * time.Millisecond)
			os.Exit(1)
		}()
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":  true,
		"pod": targetPod,
	})
}

// rootHandler (/):
// Atiende la ruta principal. Detecta si el cliente es "curl" (consola) o un navegador web:
// - Si es curl, responde texto plano sencillo para las pruebas en terminal.
// - Si es un navegador, devuelve la interfaz gráfica HTML interactiva.
func rootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")

	// Si se consulta desde curl en la terminal, devolver texto plano clásico
	if strings.Contains(strings.ToLower(r.Header.Get("User-Agent")), "curl") {
		count := atomic.AddUint64(&requestCount, 1)
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "desconocido"
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "========================================\n")
		fmt.Fprintf(w, "  DEMO KUBERNETES - SISTEMAS DISTRIBUIDOS\n")
		fmt.Fprintf(w, "========================================\n")
		fmt.Fprintf(w, "Pod (Hostname): %s\n", hostname)
		fmt.Fprintf(w, "Peticiones atendidas por este pod: %d\n", count)
		fmt.Fprintf(w, "Ruta consultada: %s\n", r.URL.Path)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

// ============================================================================
// SECCIÓN 4: FRONTEND (INTERFAZ WEB EMBEBIDA EN HTML + CSS + JS)
// ============================================================================
const indexHTML = `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Demo Kubernetes - Sistemas Distribuidos</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: system-ui, -apple-system, sans-serif;
    background: #f4f6f9;
    color: #1e293b;
    padding: 24px;
    max-width: 960px;
    margin: 0 auto;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #fff;
    padding: 16px 20px;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    margin-bottom: 20px;
  }
  header h1 { font-size: 1.25rem; color: #0f172a; }
  header p { font-size: 0.85rem; color: #64748b; margin-top: 2px; }

  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 20px;
  }
  @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }

  .card {
    background: #fff;
    padding: 18px 20px;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  }
  .card h2 {
    font-size: 0.95rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #475569;
    margin-bottom: 12px;
  }

  .active-pod-box {
    padding: 14px;
    border-radius: 8px;
    background: #f8fafc;
    border-left: 5px solid #3b82f6;
    margin-bottom: 14px;
    transition: all 0.2s ease;
  }
  .active-pod-label { font-size: 0.8rem; color: #64748b; }
  .active-pod-name {
    font-size: 1.2rem;
    font-weight: 700;
    font-family: monospace;
    margin: 4px 0;
    word-break: break-all;
  }
  .active-pod-meta { font-size: 0.85rem; color: #475569; }

  .btn-row { display: flex; gap: 8px; flex-wrap: wrap; }
  button {
    cursor: pointer;
    border: none;
    border-radius: 6px;
    padding: 9px 14px;
    font-size: 0.88rem;
    font-weight: 600;
    transition: opacity 0.15s, transform 0.05s;
  }
  button:active { transform: scale(0.98); }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-primary { background: #2563eb; color: #fff; }
  .btn-secondary { background: #e2e8f0; color: #1e293b; }
  .btn-success { background: #16a34a; color: #fff; }
  .btn-danger { background: #dc2626; color: #fff; }
  .btn-sm { padding: 5px 10px; font-size: 0.78rem; }

  .scale-box {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #f8fafc;
    padding: 12px 16px;
    border-radius: 8px;
    margin-bottom: 12px;
  }
  .scale-count { font-size: 1.4rem; font-weight: 700; color: #0f172a; }

  .pods-list { display: flex; flex-direction: column; gap: 10px; }
  .pod-item {
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 12px 14px;
    transition: border-color 0.2s, background 0.2s;
  }
  .pod-item.highlight {
    background: #eff6ff;
  }
  .pod-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
  }
  .pod-title {
    display: flex;
    align-items: center;
    gap: 8px;
    font-family: monospace;
    font-weight: 600;
    font-size: 0.92rem;
  }
  .dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }
  .badge {
    font-family: system-ui, sans-serif;
    font-size: 0.72rem;
    padding: 2px 7px;
    border-radius: 99px;
    font-weight: 600;
  }
  .badge-running { background: #dcfce7; color: #15803d; }
  .badge-pending { background: #fef9c3; color: #a16207; }
  .badge-terminating { background: #fee2e2; color: #b91c1c; }

  .bar-bg {
    height: 8px;
    background: #f1f5f9;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 4px;
  }
  .bar-fill {
    height: 100%;
    width: 0%;
    transition: width 0.25s ease;
  }
  .pod-stats {
    display: flex;
    justify-content: space-between;
    font-size: 0.78rem;
    color: #64748b;
  }

  .log-list {
    list-style: none;
    font-family: monospace;
    font-size: 0.82rem;
    max-height: 170px;
    overflow-y: auto;
  }
  .log-list li {
    padding: 5px 8px;
    border-bottom: 1px solid #f1f5f9;
    display: flex;
    justify-content: space-between;
  }
</style>
</head>
<body>

<header>
  <div>
    <h1>Demo Kubernetes — Balanceo y Alta Disponibilidad</h1>
    <p>Visualización de distribución de peticiones, escalado horizontal y auto-recuperación (Self-Healing)</p>
  </div>
  <div>
    <button class="btn-secondary btn-sm" onclick="resetStats()">Reiniciar contadores</button>
  </div>
</header>

<div class="grid">
  <!-- Panel de Balanceo de Carga -->
  <div class="card">
    <h2>1. Balanceo de Carga (Tráfico)</h2>
    <div id="activePodBox" class="active-pod-box">
      <div class="active-pod-label">Último nodo (Pod) en responder:</div>
      <div id="lastPodName" class="active-pod-name">Cargando...</div>
      <div id="lastPodMeta" class="active-pod-meta">Esperando petición...</div>
    </div>
    <div class="btn-row">
      <button class="btn-primary" onclick="sendPing()">Enviar Petición</button>
      <button class="btn-secondary" onclick="sendBurst(10)">Ráfaga x10</button>
      <button id="autoBtn" class="btn-secondary" onclick="toggleAuto()">Auto: OFF</button>
    </div>
  </div>

  <!-- Panel de Escalado -->
  <div class="card">
    <h2>2. Escalado de Nodos (Deployment)</h2>
    <div class="scale-box">
      <div>
        <div style="font-size:0.8rem; color:#64748b;">Réplicas deseadas en K8s</div>
        <div id="replicaCount" class="scale-count">-</div>
      </div>
      <div class="btn-row">
        <button class="btn-secondary" onclick="changeScale(-1)">-1 Nodo</button>
        <button class="btn-primary" onclick="changeScale(1)">+1 Nodo</button>
      </div>
    </div>
    <div class="btn-row">
      <button class="btn-secondary btn-sm" onclick="setScale(1)">1 Réplica</button>
      <button class="btn-secondary btn-sm" onclick="setScale(2)">2 Réplicas</button>
      <button class="btn-secondary btn-sm" onclick="setScale(4)">4 Réplicas</button>
      <button class="btn-secondary btn-sm" onclick="setScale(6)">6 Réplicas</button>
    </div>
  </div>
</div>

<!-- Lista de Pods y Apagado -->
<div class="card" style="margin-bottom: 20px;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
    <h2 style="margin-bottom:0;">3. Estado de los Nodos (Pods) y Simulación de Fallas</h2>
    <span style="font-size:0.8rem; color:#64748b;" id="totalHitsLabel">Total peticiones en sesión: 0</span>
  </div>
  <div id="podsContainer" class="pods-list">
    <div style="color:#64748b; font-size:0.9rem;">Consultando estado del clúster...</div>
  </div>
</div>

<!-- Historial reciente -->
<div class="card">
  <h2>Historial de Respuestas Recientes</h2>
  <ul id="logList" class="log-list"></ul>
</div>

<script>
  // Paleta de colores para asignarle un color único a cada Pod y distinguirlos visualmente
  const palette = ['#2563eb', '#16a34a', '#d97706', '#9333ea', '#0891b2', '#db2777', '#4f46e5', '#0d9488'];
  const podColors = {};
  let colorIdx = 0;

  // Variables de estado de la sesión en el navegador
  let sessionHits = {};   // Cantidad de peticiones recibidas por cada pod en esta sesión
  let podTotalCount = {}; // Contador histórico interno que reporta cada pod
  let killedPods = {};    // Registro de pods que el usuario mandó a apagar
  let totalSessionRequests = 0;
  let currentReplicas = 2;
  let lastActivePod = '';
  let clusterPods = [];
  let autoTimer = null;
  let reqSeq = 0;

  // Asigna o recupera el color identificador de un Pod
  function getColor(podName) {
    if (!podColors[podName]) {
      podColors[podName] = palette[colorIdx % palette.length];
      colorIdx++;
    }
    return podColors[podName];
  }

  // Envía una petición a /api/ping para demostrar cómo Kubernetes balancea la carga
  async function sendPing() {
    try {
      const res = await fetch('/api/ping?t=' + Date.now(), { cache: 'no-store' });
      const data = await res.json();

      reqSeq++;
      totalSessionRequests++;
      lastActivePod = data.pod;
      sessionHits[data.pod] = (sessionHits[data.pod] || 0) + 1;
      podTotalCount[data.pod] = data.count;
      currentReplicas = data.replicas || currentReplicas;
      if (data.pods && data.pods.length > 0) {
        clusterPods = data.pods;
      }

      const color = getColor(data.pod);
      const box = document.getElementById('activePodBox');
      box.style.borderLeftColor = color;
      document.getElementById('lastPodName').textContent = data.pod;
      document.getElementById('lastPodName').style.color = color;
      document.getElementById('lastPodMeta').textContent =
        'Peticiones atendidas por este pod: ' + data.count + ' (Hora: ' + data.time + ')';

      addLog(reqSeq, data.pod, data.count, data.time, color);
      renderCluster();
    } catch (err) {
      console.error('Error en petición:', err);
    }
  }

  // Envía N peticiones seguidas (ráfaga) para ver rápido la distribución entre los pods
  async function sendBurst(n) {
    for (let i = 0; i < n; i++) {
      await sendPing();
      await new Promise(r => setTimeout(r, 90));
    }
  }

  // Activa o desactiva el envío automático de 1 petición por segundo
  function toggleAuto() {
    const btn = document.getElementById('autoBtn');
    if (autoTimer) {
      clearInterval(autoTimer);
      autoTimer = null;
      btn.textContent = 'Auto: OFF';
      btn.className = 'btn-secondary';
    } else {
      sendPing();
      autoTimer = setInterval(sendPing, 900);
      btn.textContent = 'Auto: ON (1/s)';
      btn.className = 'btn-success';
    }
  }

  // Consulta periódicamente el estado de los pods en Kubernetes sin sumar tráfico
  async function fetchStatus() {
    try {
      const res = await fetch('/api/status?t=' + Date.now(), { cache: 'no-store' });
      const data = await res.json();
      if (data.replicas) currentReplicas = data.replicas;
      if (data.pods) clusterPods = data.pods;
      renderCluster();
    } catch (e) {}
  }

  // Llama a /api/scale para escalar hacia arriba o abajo la cantidad de réplicas en Kubernetes
  async function setScale(target) {
    if (target < 1) target = 1;
    if (target > 10) target = 10;
    currentReplicas = target;
    document.getElementById('replicaCount').textContent = currentReplicas;
    try {
      await fetch('/api/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ replicas: target })
      });
      setTimeout(fetchStatus, 300);
    } catch (e) {
      console.error('Error al escalar:', e);
    }
  }

  function changeScale(delta) {
    setScale(currentReplicas + delta);
  }

  // Llama a /api/kill para apagar/eliminar un Pod específico y observar el Self-Healing
  async function killPod(podName) {
    killedPods[podName] = 'Terminating';
    // Marcar inmediatamente como Terminating en la vista actual
    clusterPods.forEach(p => {
      if (p.name === podName) p.status = 'Terminating';
    });
    renderCluster();

    try {
      await fetch('/api/kill', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pod: podName })
      });
      setTimeout(fetchStatus, 400);
      setTimeout(fetchStatus, 1200);
    } catch (e) {
      setTimeout(fetchStatus, 500);
    }
  }

  // Limpia los contadores de la sesión actual y quita de la vista los pods que ya fueron apagados
  function resetStats() {
    sessionHits = {};
    killedPods = {};
    totalSessionRequests = 0;
    document.getElementById('logList').innerHTML = '';
    fetchStatus();
  }

  // Agrega una línea al historial de respuestas recientes
  function addLog(seq, pod, count, time, color) {
    const ul = document.getElementById('logList');
    const li = document.createElement('li');
    li.innerHTML = '<span><strong>#' + seq + '</strong> &rarr; <span style="color:' + color + ';font-weight:700;">' + pod + '</span></span>' +
                   '<span style="color:#64748b;">Pet. del pod: ' + count + ' | ' + time + '</span>';
    ul.prepend(li);
    while (ul.children.length > 12) {
      ul.removeChild(ul.lastChild);
    }
  }

  // Dibuja la lista de Pods, sus estados (Running, Iniciando, Apagando, Apagado) y las barras de % de tráfico
  function renderCluster() {
    document.getElementById('replicaCount').textContent = currentReplicas;
    document.getElementById('totalHitsLabel').textContent = 'Total peticiones en sesión: ' + totalSessionRequests;

    const container = document.getElementById('podsContainer');
    container.innerHTML = '';

    // Crear lista combinada sin mutar clusterPods original
    const displayPods = clusterPods.map(p => ({ ...p }));
    const activeNames = new Set(displayPods.map(p => p.name));

    // Agregar pods que fueron apagados o vistos antes en la sesión y ya no existen en K8s
    const historicalNames = new Set([...Object.keys(sessionHits), ...Object.keys(killedPods)]);
    historicalNames.forEach(name => {
      if (!activeNames.has(name)) {
        displayPods.push({ name: name, status: 'Terminated', ready: false });
      }
    });

    displayPods.forEach(pod => {
      const color = getColor(pod.name);
      const hits = sessionHits[pod.name] || 0;
      const pct = totalSessionRequests > 0 ? Math.round((hits / totalSessionRequests) * 100) : 0;
      const isLast = (pod.name === lastActivePod && pod.status !== 'Terminated');

      let badgeClass = 'badge-running';
      let statusText = pod.status || 'Running';
      let btnText = 'Apagar Nodo';
      let btnClass = 'btn-danger btn-sm';
      let btnDisabled = '';

      if (statusText === 'Terminated') {
        badgeClass = 'badge-terminating';
        statusText = 'Apagado';
        btnText = 'Apagado';
        btnClass = 'btn-secondary btn-sm';
        btnDisabled = 'disabled';
      } else if (statusText === 'Terminating') {
        badgeClass = 'badge-pending';
        statusText = 'Apagando...';
        btnText = 'Apagando...';
        btnClass = 'btn-danger btn-sm';
        btnDisabled = 'disabled';
      } else if (!pod.ready) {
        badgeClass = 'badge-pending';
        statusText = 'Iniciando...';
      }

      const item = document.createElement('div');
      item.className = 'pod-item' + (isLast ? ' highlight' : '');
      if (isLast) item.style.borderColor = color;
      if (pod.status === 'Terminated') {
        item.style.opacity = '0.65';
        item.style.background = '#f8fafc';
      }

      item.innerHTML =
        '<div class="pod-top">' +
          '<div class="pod-title">' +
            '<span class="dot" style="background:' + (pod.status === 'Terminated' ? '#94a3b8' : color) + '"></span>' +
            '<span style="' + (pod.status === 'Terminated' ? 'text-decoration:line-through;color:#64748b;' : '') + '">' + pod.name + '</span>' +
            '<span class="badge ' + badgeClass + '">' + statusText + '</span>' +
          '</div>' +
          '<button class="' + btnClass + '" ' + btnDisabled + ' onclick="killPod(\'' + pod.name + '\')">' +
            btnText +
          '</button>' +
        '</div>' +
        '<div class="bar-bg">' +
          '<div class="bar-fill" style="width:' + pct + '%; background:' + color + ';"></div>' +
        '</div>' +
        '<div class="pod-stats">' +
          '<span>Tráfico en sesión: <strong>' + hits + '</strong> (' + pct + '%)</span>' +
          '<span>Total histórico del pod: <strong>' + (podTotalCount[pod.name] || 0) + '</strong></span>' +
        '</div>';

      container.appendChild(item);
    });
  }

  // Primera petición al cargar y actualización periódica del estado de los pods
  sendPing();
  setInterval(fetchStatus, 2000);
</script>
</body>
</html>
`

// ============================================================================
// SECCIÓN 5: FUNCIÓN PRINCIPAL E INICIO DEL SERVIDOR HTTP
// ============================================================================
func main() {
	// Registrar las rutas del servidor HTTP
	mux := http.NewServeMux()
	mux.HandleFunc("/", rootHandler)           // Interfaz web (o texto plano si se usa curl)
	mux.HandleFunc("/api/ping", pingHandler)   // Envía una petición balanceada y suma +1 al pod que la recibe
	mux.HandleFunc("/api/status", statusHandler) // Consulta el estado de los pods en K8s sin sumar tráfico
	mux.HandleFunc("/api/scale", scaleHandler) // Cambia la cantidad de réplicas del Deployment
	mux.HandleFunc("/api/kill", killHandler)   // Elimina/apaga un pod para demostrar Self-Healing

	port := ":8080"
	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	// IMPORTANTE PARA LA DEMO:
	// En Kubernetes, el Service balancea a nivel de conexión TCP (Layer 4).
	// Si el navegador reutiliza la misma conexión TCP (HTTP Keep-Alive), todas las peticiones
	// irían siempre al mismo Pod. Al desactivar Keep-Alive, cada petición abre una nueva
	// conexión TCP y Kubernetes la reparte entre todos los Pods activos.
	server.SetKeepAlivesEnabled(false)

	fmt.Printf("Servidor iniciado en el puerto %s...\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Error al iniciar el servidor: %s\n", err)
	}
}
