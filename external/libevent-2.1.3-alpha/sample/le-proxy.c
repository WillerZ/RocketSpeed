/*
  This example code shows how to write an (optionally encrypting) SSL proxy
  with Libevent's bufferevent layer.

  XXX It's a little ugly and should probably be cleaned up.
 */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#endif

#include <event2/bufferevent_ssl.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/rand.h>

static struct event_base *base;
static struct sockaddr_storage listen_on_addr;
static struct sockaddr_storage connect_to_addr;
static int connect_to_addrlen;
static int use_wrapper = 1;

static SSL_CTX *ssl_ctx = NULL;

#define MAX_OUTPUT (512*1024)

static void drained_writecb(struct bufferevent *bev, void *ctx);
static void eventcb(struct bufferevent *bev, short what, void *ctx);

static void
readcb(struct bufferevent *bev, void *ctx)
{
	struct bufferevent *partner = ctx;
	struct evbuffer *src, *dst;
	size_t len;
	src = ld_bufferevent_get_input(bev);
	len = ld_evbuffer_get_length(src);
	if (!partner) {
		ld_evbuffer_drain(src, len);
		return;
	}
	dst = ld_bufferevent_get_output(partner);
	ld_evbuffer_add_buffer(dst, src);

	if (ld_evbuffer_get_length(dst) >= MAX_OUTPUT) {
		/* We're giving the other side data faster than it can
		 * pass it on.  Stop reading here until we have drained the
		 * other side to MAX_OUTPUT/2 bytes. */
		ld_bufferevent_setcb(partner, readcb, drained_writecb,
		    eventcb, bev);
		ld_bufferevent_setwatermark(partner, EV_WRITE, MAX_OUTPUT/2,
		    MAX_OUTPUT);
		ld_bufferevent_disable(bev, EV_READ);
	}
}

static void
drained_writecb(struct bufferevent *bev, void *ctx)
{
	struct bufferevent *partner = ctx;

	/* We were choking the other side until we drained our outbuf a bit.
	 * Now it seems drained. */
	ld_bufferevent_setcb(bev, readcb, NULL, eventcb, partner);
	ld_bufferevent_setwatermark(bev, EV_WRITE, 0, 0);
	if (partner)
		ld_bufferevent_enable(partner, EV_READ);
}

static void
close_on_finished_writecb(struct bufferevent *bev, void *ctx)
{
	struct evbuffer *b = ld_bufferevent_get_output(bev);

	if (ld_evbuffer_get_length(b) == 0) {
		ld_bufferld_event_free(bev);
	}
}

static void
eventcb(struct bufferevent *bev, short what, void *ctx)
{
	struct bufferevent *partner = ctx;

	if (what & (BEV_EVENT_EOF|BEV_EVENT_ERROR)) {
		if (what & BEV_EVENT_ERROR) {
			unsigned long err;
			while ((err = (bufferevent_get_openssl_error(bev)))) {
				const char *msg = (const char*)
				    ERR_reason_error_string(err);
				const char *lib = (const char*)
				    ERR_lib_error_string(err);
				const char *func = (const char*)
				    ERR_func_error_string(err);
				fprintf(stderr,
				    "%s in %s %s\n", msg, lib, func);
			}
			if (errno)
				perror("connection error");
		}

		if (partner) {
			/* Flush all pending data */
			readcb(bev, ctx);

			if (ld_evbuffer_get_length(
				    ld_bufferevent_get_output(partner))) {
				/* We still have to flush data from the other
				 * side, but when that's done, close the other
				 * side. */
				ld_bufferevent_setcb(partner,
				    NULL, close_on_finished_writecb,
				    eventcb, NULL);
				ld_bufferevent_disable(partner, EV_READ);
			} else {
				/* We have nothing left to say to the other
				 * side; close it. */
				ld_bufferld_event_free(partner);
			}
		}
		ld_bufferld_event_free(bev);
	}
}

static void
syntax(void)
{
	fputs("Syntax:\n", stderr);
	fputs("   le-proxy [-s] [-W] <listen-on-addr> <connect-to-addr>\n", stderr);
	fputs("Example:\n", stderr);
	fputs("   le-proxy 127.0.0.1:8888 1.2.3.4:80\n", stderr);

	exit(1);
}

static void
accept_cb(struct evconnlistener *listener, evutil_socket_t fd,
    struct sockaddr *a, int slen, void *p)
{
	struct bufferevent *b_out, *b_in;
	/* Create two linked bufferevent objects: one to connect, one for the
	 * new connection */
	b_in = ld_bufferevent_socket_new(base, fd,
	    BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);

	if (!ssl_ctx || use_wrapper)
		b_out = ld_bufferevent_socket_new(base, -1,
		    BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);
	else {
		SSL *ssl = SSL_new(ssl_ctx);
		b_out = bufferevent_openssl_socket_new(base, -1, ssl,
		    BUFFEREVENT_SSL_CONNECTING,
		    BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);
	}

	assert(b_in && b_out);

	if (ld_bufferevent_socket_connect(b_out,
		(struct sockaddr*)&connect_to_addr, connect_to_addrlen)<0) {
		perror("ld_bufferevent_socket_connect");
		ld_bufferld_event_free(b_out);
		ld_bufferld_event_free(b_in);
		return;
	}

	if (ssl_ctx && use_wrapper) {
		struct bufferevent *b_ssl;
		SSL *ssl = SSL_new(ssl_ctx);
		b_ssl = bufferevent_openssl_filter_new(base,
		    b_out, ssl, BUFFEREVENT_SSL_CONNECTING,
		    BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);
		if (!b_ssl) {
			perror("Bufferevent_openssl_new");
			ld_bufferld_event_free(b_out);
			ld_bufferld_event_free(b_in);
		}
		b_out = b_ssl;
	}

	ld_bufferevent_setcb(b_in, readcb, NULL, eventcb, b_out);
	ld_bufferevent_setcb(b_out, readcb, NULL, eventcb, b_in);

	ld_bufferevent_enable(b_in, EV_READ|EV_WRITE);
	ld_bufferevent_enable(b_out, EV_READ|EV_WRITE);
}

int
main(int argc, char **argv)
{
	int i;
	int socklen;

	int use_ssl = 0;
	struct evconnlistener *listener;

	if (argc < 3)
		syntax();

	for (i=1; i < argc; ++i) {
		if (!strcmp(argv[i], "-s")) {
			use_ssl = 1;
		} else if (!strcmp(argv[i], "-W")) {
			use_wrapper = 0;
		} else if (argv[i][0] == '-') {
			syntax();
		} else
			break;
	}

	if (i+2 != argc)
		syntax();

	memset(&listen_on_addr, 0, sizeof(listen_on_addr));
	socklen = sizeof(listen_on_addr);
	if (ld_evutil_parse_sockaddr_port(argv[i],
		(struct sockaddr*)&listen_on_addr, &socklen)<0) {
		int p = atoi(argv[i]);
		struct sockaddr_in *sin = (struct sockaddr_in*)&listen_on_addr;
		if (p < 1 || p > 65535)
			syntax();
		sin->sin_port = htons(p);
		sin->sin_addr.s_addr = htonl(0x7f000001);
		sin->sin_family = AF_INET;
		socklen = sizeof(struct sockaddr_in);
	}

	memset(&connect_to_addr, 0, sizeof(connect_to_addr));
	connect_to_addrlen = sizeof(connect_to_addr);
	if (ld_evutil_parse_sockaddr_port(argv[i+1],
		(struct sockaddr*)&connect_to_addr, &connect_to_addrlen)<0)
		syntax();

	base = ld_event_base_new();
	if (!base) {
		perror("ld_event_base_new()");
		return 1;
	}

	if (use_ssl) {
		int r;
		SSL_library_init();
		ERR_load_crypto_strings();
		SSL_load_error_strings();
		OpenSSL_add_all_algorithms();
		r = RAND_poll();
		if (r == 0) {
			fprintf(stderr, "RAND_poll() failed.\n");
			return 1;
		}
		ssl_ctx = SSL_CTX_new(SSLv23_method());
	}

	listener = ld_evconnlistener_new_bind(base, accept_cb, NULL,
	    LEV_OPT_CLOSE_ON_FREE|LEV_OPT_CLOSE_ON_EXEC|LEV_OPT_REUSEABLE,
	    -1, (struct sockaddr*)&listen_on_addr, socklen);

	ld_event_base_dispatch(base);

	ld_evconnlistener_free(listener);
	ld_event_base_free(base);

	return 0;
}
