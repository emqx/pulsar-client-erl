## shallow clone for speed

REBAR = rebar3
all: compile

compile:
	$(REBAR) compile

clean: distclean

ct:
	$(REBAR) ct -v

eunit:
	$(REBAR) as test eunit

xref:
	$(REBAR) xref

dialyzer:
	$(REBAR) dialyzer

distclean:
	@rm -rf _build
	@rm -f rebar.lock

dialyzer:
	$(REBAR) dialyzer

cover:
	$(REBAR) cover

coveralls:
	$(REBAR) as test coveralls send
