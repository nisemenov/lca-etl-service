// Package httpclient предоставляет удобный и переиспользуемый HTTP-клиент
// для внутренних взаимодействий приложения.
//
// Основные особенности:
//   - Поддержка базового URL и автоматического формирования путей
//   - Настраиваемые default-заголовки (например, Authorization, User-Agent)
//   - Middleware / хуки для модификации запросов перед отправкой
//   - Опциональный паттерн Option для гибкой конфигурации
//
// Пример использования:
//
//	apiClient := httpclient.NewHTTPClient(
//		&http.Client{...},
//	    "https://api.internal",
//	    httpclient.WithHeaders(map[string]string{
//	        "X-Internal-Token": "secret_token",
//	    }),
//	)
package httpclient
