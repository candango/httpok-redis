<?php

declare(strict_types=1);

final class JsonRedisSessionHandler implements SessionHandlerInterface
{
    private Redis $redis;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly int $database,
        private readonly int $ttl,
        private readonly string $prefix = 'GLOBAL_SESSION',
    ) {
    }

    public function open(string $savePath, string $sessionName): bool
    {
        $this->redis = new Redis();
        if (!$this->redis->connect($this->host, $this->port)) {
            return false;
        }
        return $this->redis->select($this->database);
    }

    public function close(): bool
    {
        return true;
    }

    public function read(string $id): string
    {
        $key = $this->key($id);
        $raw = $this->redis->get($key);
        if ($raw === false) {
            $_SESSION = ['created' => time()];
        } else {
            $_SESSION = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        }
        $this->redis->expire($key, $this->ttl);
        return session_encode() ?: '';
    }

    public function write(string $id, string $data): bool
    {
        $key = $this->key($id);
        $raw = $this->redis->get($key);
        if ($raw === false) {
            $_SESSION['created'] = time();
        } else {
            $stored = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            $_SESSION['created'] = $stored['created'] ?? time();
        }
        $_SESSION['updated'] = time();
        $payload = json_encode($_SESSION, JSON_THROW_ON_ERROR);
        return $this->redis->setex($key, $this->ttl, $payload);
    }

    public function destroy(string $id): bool
    {
        return $this->redis->del($this->key($id)) >= 0;
    }

    public function gc(int $maxLifetime): int|false
    {
        return 0;
    }

    private function key(string $id): string
    {
        return $this->prefix . ':' . $id;
    }
}

if ($argc !== 6) {
    fwrite(STDERR, "usage: php php_session.php HOST PORT SESSION_ID TTL OP\n");
    exit(2);
}

[$script, $host, $port, $sessionId, $ttl, $operation] = $argv;
$ttl = (int) $ttl;
ini_set('session.gc_maxlifetime', (string) $ttl);
session_name('PHPSESSID');
session_set_save_handler(
    new JsonRedisSessionHandler($host, (int) $port, 1, $ttl),
    true,
);
session_id($sessionId);
if (!session_start()) {
    fwrite(STDERR, "session_start failed\n");
    exit(1);
}

switch ($operation) {
    case 'seed':
        $_SESSION['UserRole'] = 'Member';
        $_SESSION['from_php'] = 'seeded';
        break;
    case 'set':
        $_SESSION['from_php'] = 'updated';
        break;
    case 'read':
        echo json_encode($_SESSION, JSON_THROW_ON_ERROR), PHP_EOL;
        break;
    case 'destroy':
        if (!session_destroy()) {
            fwrite(STDERR, "session_destroy failed\n");
            exit(1);
        }
        exit(0);
    default:
        fwrite(STDERR, "unknown operation\n");
        exit(2);
}

session_write_close();
