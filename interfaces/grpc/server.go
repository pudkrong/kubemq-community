package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/interfaces/grpc/middleware/recovery"
	"github.com/kubemq-io/kubemq-community/services"
	"go.uber.org/atomic"
	"net"
	"time"

	"github.com/kubemq-io/kubemq-community/pkg/entities"

	"github.com/pkg/errors"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"github.com/kubemq-io/kubemq-community/interfaces/grpc/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type options struct {
	port     int
	security *config.SecurityConfig
	maxSize  int
	bufSize  int
}

func recoveryFunc(p interface{}) (err error) {
	fmt.Println("crashed")
	return errors.Wrapf(entities.ErrGRPCSServerCrushed, " value: %v", p)
}
func configureServer(svc *services.SystemServices, logger *logging.Logger, opts *options, acceptTraffic *atomic.Bool) (s *grpc.Server, err error) {
	recoveryOpts := []recovery.Option{
		recovery.WithRecoveryHandler(recoveryFunc),
	}
	var connOptions []grpc.ServerOption
	connOptions = append(connOptions, grpc.MaxRecvMsgSize(opts.maxSize))
	connOptions = append(connOptions, grpc.MaxSendMsgSize(opts.maxSize))
	// Server-side keepalive: probe idle connections every 30s and close them if
	// no PONG ack arrives within 10s. Without this the server cannot detect
	// half-dead client streams, so dead subscribers lingered until the client
	// SDK itself gave up ("Channel has been shutdown"). No MaxConnectionAge is
	// set, so healthy long-lived connections are not forced to reconnect.
	connOptions = append(connOptions, grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    30 * time.Second,
		Timeout: 10 * time.Second,
	}))
	connOptions = append(connOptions, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))

	switch opts.security.Mode() {
	case config.SecurityModeTLS:
		certBlock, err := opts.security.Cert.Get()
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "invalid cert data : %s", err.Error())
		}
		keyBlock, err := opts.security.Key.Get()
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "invalid key data: %s", err.Error())
		}
		certFromKeyPair, err := tls.X509KeyPair(certBlock, keyBlock)
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "error creating c509k certification: %s", err.Error())
		}
		creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certFromKeyPair}, MinVersion: tls.VersionTLS12})
		connOptions = append(connOptions, grpc.Creds(creds))
	case config.SecurityModeMTLS:
		certBlock, err := opts.security.Cert.Get()
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "invalid cert data : %s", err.Error())
		}
		keyBlock, err := opts.security.Key.Get()
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "invalid key data: %s", err.Error())
		}
		certFromKeyPair, err := tls.X509KeyPair(certBlock, keyBlock)
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "error creating c509k certification: %s", err.Error())
		}
		caBlock, err := opts.security.Ca.Get()
		if err != nil {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "invalid ca data: %s", err.Error())
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caBlock) {
			return nil, errors.Wrapf(entities.ErrLoadTLSCertificate, "credentials: failed to append certificates")
		}
		creds := credentials.NewTLS(&tls.Config{
			RootCAs:      certPool,
			Certificates: []tls.Certificate{certFromKeyPair},
			MinVersion:   tls.VersionTLS12,
		},
		)
		connOptions = append(connOptions, grpc.Creds(creds))
	}

	connOptions = append(connOptions, grpc.StreamInterceptor(middleware.ChainStreamServer(
		recovery.StreamServerInterceptor(recoveryOpts...),
		middleware.StreamLoggerServerInterceptor(logger),
		middleware.StreamTrafficServerInterceptor(acceptTraffic),
	)))
	connOptions = append(connOptions, grpc.UnaryInterceptor(middleware.ChainUnaryServer(
		recovery.UnaryServerInterceptor(recoveryOpts...),
		middleware.UnaryLoggerServerInterceptor(logger),
		middleware.UnaryTrafficServerInterceptor(acceptTraffic),
	)))

	s = grpc.NewServer(connOptions...)

	reflection.Register(s)

	return
}

func runServer(s *grpc.Server, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go func() {
		_ = s.Serve(lis)
	}()

	return nil
}
