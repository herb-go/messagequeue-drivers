package nsq_test

import (
	"testing"
	"time"

	_ "github.com/herb-go/herbconfig/loader/drivers/jsonconfig"

	"github.com/herb-go/herbconfig/loader"
	"github.com/herb-go/messagequeue"
	"github.com/herb-go/messagequeue/messagequeuetestutil"
)

var ttl = 300 * time.Microsecond

func newTestBroker() *messagequeue.Broker {
	b := messagequeue.NewBroker()
	c := messagequeue.NewBrokerConfig()
	err := loader.LoadConfig("json", []byte(testConfig), c)
	if err != nil {
		panic(err)
	}
	err = c.ApplyTo(b)
	if err != nil {
		panic(err)
	}
	return b

}
func newLookupTestBroker() *messagequeue.Broker {
	b := messagequeue.NewBroker()
	c := messagequeue.NewBrokerConfig()
	err := loader.LoadConfig("json", []byte(testLookupConfig), c)
	if err != nil {
		panic(err)
	}
	err = c.ApplyTo(b)
	if err != nil {
		panic(err)
	}
	return b
}

func TestBroker(t *testing.T) {
	b := newTestBroker()
	ctx := messagequeuetestutil.NewTestContext()
	messagequeuetestutil.TestBroker(b, 5, "test", ctx, ttl, nil)
	if len(ctx.Msgs) != 3 || len(ctx.Errors) != 1 {
		t.Fatal(ctx.Msgs, ctx.Errors)
	}
}
func TestLookupBroker(t *testing.T) {
	b := newLookupTestBroker()
	ctx := messagequeuetestutil.NewTestContext()
	messagequeuetestutil.TestBroker(b, 5, "test", ctx, ttl, nil)
	if len(ctx.Msgs) != 3 || len(ctx.Errors) != 1 {
		t.Fatal(ctx.Msgs, ctx.Errors)
	}
}
